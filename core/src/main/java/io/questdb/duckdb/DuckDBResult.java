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
import io.questdb.std.str.DirectUtf8StringZ;
//    public static native long resultGetError(long result);
//    public static native long resultFetchChunk(long result);
//    public static native void resultDestroy(long resultPtr);
//    public static native long resultColumnName(long resultPtr, long col);
//    public static native int resultColumnPhysicalType(long resultPtr, long col);
//    public static native int resultColumnLogicalType(long resultPtr, long col);
//    public static native long resultColumnCount(long resultPtr);
//    public static native long resultGetMaterialized(long resultPtr);
//    public static native long resultRowCount(long resultPtr);
//    public static native long resultError(long resultPtr);
//    public static native int resultGetQueryResultType(long resultPtr);
//    public static native long resultGetDataChunk(long resultPtr, long chunkIndex);
//    public static native long resultDataChunkCount(long resultPtr);
public class DuckDBResult implements QuietCloseable {
    private final DirectUtf8StringZ text = new DirectUtf8StringZ();
    private long result;


    public void of(long result) {
        assert result != 0;

        if (this.result != 0) {
            DuckDB.resultDestroy(this.result);
        }
        this.result = result;
    }

    public boolean isClosed() {
        return result == 0;
    }

    @Override
    public void close() {
        if (result != 0) {
            DuckDB.resultDestroy(result);
            result = 0;
        }
    }

    public DirectUtf8StringZ getErrorText() {
        text.of(DuckDB.resultGetError(result));
        return text;
    }

    public DuckDBDataChunk fetchChunk() {
        long ptr = DuckDB.resultFetchChunk(result);
        if (ptr == 0) {
            return null;
        }
        DuckDBDataChunk chunk = new DuckDBDataChunk();
        chunk.of(ptr);
        return chunk;
    }

}
