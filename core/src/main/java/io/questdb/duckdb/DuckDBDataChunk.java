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

import io.questdb.std.QuietCloseable;

public class DuckDBDataChunk implements QuietCloseable {
    private long chunk;

    @Override
    public void close() {
        if (chunk != 0) {
            DuckDB.dataChunkDestroy(chunk);
            chunk = 0;
        }
    }

    public void of(long chunk) {
        assert chunk != 0;
        if (this.chunk != 0) {
            DuckDB.dataChunkDestroy(this.chunk);
        }
        this.chunk = chunk;
    }

    public long getRowCount() {
        return DuckDB.dataChunkGetSize(chunk);
    }

    public long getColumnCount() {
        return DuckDB.dataChunkGetColumnCount(chunk);
    }

    public int getColumnType(long colIdx) {
        long ptr = DuckDB.dataChunkGetVector(chunk, colIdx);
        if (ptr == 0) {
            return 0;
        }
        int duckType = DuckDB.vectorGetColumnPhysicalType(ptr);
        return DuckDB.getQdbColumnType(duckType);
    }

    public long getColumnDataAddr(long colIdx) {
        long ptr = DuckDB.dataChunkGetVector(chunk, colIdx);
        if (ptr == 0) {
            return 0;
        }
        return DuckDB.vectorGetData(ptr);
    }

    public long getColumnValidityAddr(long colIdx) {
        long ptr = DuckDB.dataChunkGetVector(chunk, colIdx);
        if (ptr == 0) {
            return 0;
        }
        return DuckDB.vectorGetValidity(ptr);
    }
}
