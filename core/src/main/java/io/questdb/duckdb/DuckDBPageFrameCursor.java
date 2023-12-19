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
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.str.DirectUtf8StringZ;
import org.jetbrains.annotations.Nullable;

public class DuckDBPageFrameCursor implements PageFrameCursor {
    private final PageFrameImpl frame = new PageFrameImpl();
    private long preparedStmt;
    private long queryResultPtr;
    private long currentDataChunk;
    private int currentDataChunkIndex;
    private boolean exhausted;

    public void of(long preparedStmt) throws SqlException {
        assert preparedStmt != 0;
        this.preparedStmt = preparedStmt;
        this.close();
        // this owns queryResult and is responsible for its destruction
        this.queryResultPtr = DuckDB.preparedExecute(preparedStmt);
        this.exhausted = false;
        long err = DuckDB.resultGetError(this.queryResultPtr);
        if (err != 0) {
            DirectUtf8StringZ utf8String = new DirectUtf8StringZ();
            throw SqlException.$(1, utf8String.of(err).toString());
        }
    }

    @Override
    public void close() {
        if (currentDataChunk != 0) {
            DuckDB.dataChunkDestroy(currentDataChunk);
        }
        if (queryResultPtr != 0) {
            DuckDB.resultDestroy(queryResultPtr);
        }

        queryResultPtr = 0;
        currentDataChunk = 0;
        currentDataChunkIndex = -1;
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
        return Rows.toRowID(currentDataChunkIndex, rowIndex);
    }

    @Override
    public @Nullable PageFrameImpl next() {
        exhausted = true; // dirty hack to prevent toTop to re-execute query
        // release previous chunk
        if (currentDataChunk != 0) {
            DuckDB.dataChunkDestroy(currentDataChunk);
        }
        currentDataChunk = DuckDB.resultFetchChunk(queryResultPtr);
        if (currentDataChunk != 0) {
            currentDataChunkIndex++;
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
        if (exhausted) {
            this.close();
            this.queryResultPtr = DuckDB.preparedExecute(preparedStmt); // TODO: how to report error ??
            this.exhausted = false;
        }
    }

    public class PageFrameImpl implements PageFrame {
        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public int getColumnShiftBits(int columnIndex) {
            long duckTypes = DuckDB.resultColumnTypes(queryResultPtr, columnIndex);
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
            long vec = DuckDB.dataChunkGetVector(currentDataChunk, columnIndex);
            return DuckDB.vectorGetData(vec);
        }

        public long getValidityMaskAddress(int columnIndex) {
            long vec = DuckDB.dataChunkGetVector(currentDataChunk, columnIndex);
            return DuckDB.vectorGetValidity(vec);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return DuckDB.dataChunkGetSize(currentDataChunk) << getColumnShiftBits(columnIndex);
        }

        @Override
        public long getPartitionHi() {
            return DuckDB.dataChunkGetSize(currentDataChunk);
        }

        @Override
        public int getPartitionIndex() {
            return currentDataChunkIndex;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }
}
