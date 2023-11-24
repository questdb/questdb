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

import io.questdb.std.Unsafe;

public class DuckDB {
    public static final int DUCKDB_TYPE_INVALID = 0;
    // bool
    public static final int DUCKDB_TYPE_BOOLEAN = 1;
    // int8_t
    public static final int  DUCKDB_TYPE_TINYINT = 2;
    // int16_t
    public static final int  DUCKDB_TYPE_SMALLINT = 3;
    // int32_t
    public static final int DUCKDB_TYPE_INTEGER = 4;
    // int64_t
    public static final int DUCKDB_TYPE_BIGINT = 5;
    // uint8_t
    public static final int DUCKDB_TYPE_UTINYINT = 6;
    // uint16_t
    public static final int DUCKDB_TYPE_USMALLINT = 7;
    // uint32_t
    public static final int DUCKDB_TYPE_UINTEGER = 8;
    // uint64_t
    public static final int DUCKDB_TYPE_UBIGINT = 9;
    // float
    public static final int DUCKDB_TYPE_FLOAT = 10;
    // double
    public static final int DUCKDB_TYPE_DOUBLE = 11;
    // duckdb_timestamp = 0; in microseconds
    public static final int DUCKDB_TYPE_TIMESTAMP = 12;
    // duckdb_date
    public static final int DUCKDB_TYPE_DATE = 13;
    // duckdb_time
    public static final int DUCKDB_TYPE_TIME = 14;
    // duckdb_interval
    public static final int DUCKDB_TYPE_INTERVAL = 15;
    // duckdb_hugeint
    public static final int DUCKDB_TYPE_HUGEINT = 16;
    // const char*
    public static final int DUCKDB_TYPE_VARCHAR = 17;
    // duckdb_blob
    public static final int DUCKDB_TYPE_BLOB = 18;
    // decimal
    public static final int DUCKDB_TYPE_DECIMAL = 19;
    // duckdb_timestamp = 0; in seconds
    public static final int DUCKDB_TYPE_TIMESTAMP_S = 20;
    // duckdb_timestamp = 0; in milliseconds
    public static final int DUCKDB_TYPE_TIMESTAMP_MS = 21;
    // duckdb_timestamp = 0; in nanoseconds
    public static final int DUCKDB_TYPE_TIMESTAMP_NS = 22;
    // enum type = 0; only useful as logical type
    public static final int DUCKDB_TYPE_ENUM = 23;
    // list type = 0; only useful as logical type
    public static final int DUCKDB_TYPE_LIST = 24;
    // struct type = 0; only useful as logical type
    public static final int DUCKDB_TYPE_STRUCT = 25;
    // map type = 0; only useful as logical type
    public static final int DUCKDB_TYPE_MAP = 26;
    // duckdb_hugeint
    public static final int DUCKDB_TYPE_UUID = 27;
    // union type = 0; only useful as logical type
    public static final int DUCKDB_TYPE_UNION = 28;
    // duckdb_bit
    public static final int DUCKDB_TYPE_BIT = 29;

    public static boolean validityRowIsValid(long validityPtr, long row) {
        if (validityPtr == 0) {
            return true;
        }
        long entry_idx = row / 64;
        long idx_in_entry = row % 64;
        long mask = Unsafe.getUnsafe().getLong(validityPtr + entry_idx * 8);
        return (mask & (1L << idx_in_entry)) != 0;
    }

    // accept db path, (0, 0) means in memory database
    // return 0 on failure, valid dbPtr on success
    public static native long databaseOpen(long pathPtr, long pathSize);
    public static native long databaseOpenExt(long pathPtr, long pathSize, long configPtr);
    public static native void databaseClose(long dbPtr);

    // return 0 on failure, valid connectionPtr on success
    public static native long databaseConnect(long dbPtr);

    public static native void connectionInterrupt(long connectionPtr);
    public static native double connectionQueryProgress(long connectionPtr);
    public static native void connectionDisconnect(long connectionPrt);
    // return 0 on failure, valid resultPtr on success
    // resultPtr must be destroyed with resultDestroy
    public static native long connectionQuery(long connectionPtr, long queryPtr, long querySize);
    // return 0 on success, 1 on failure
    public static native long connectionExec(long connectionPtr, long queryPtr, long querySize);

    public static native long configCreate();
    public static native long configSet(long configPtr, long namePtr, long nameSize, long optionPtr, long optionSize);
    public static native void configDestroy(long configPtr);

    public static native void resultDestroy(long resultPtr);
    // return zero terminated utf8 string on success, 0 on failure
    public static native long resultColumnName(long resultPtr, long col);
    public static native int resultColumnType(long resultPtr, long col);
    public static native long resultColumnCount(long resultPtr);
    public static native long resultRowCount(long resultPtr);
    public static native long resultColumnData(long resultPtr, long col);
    // return uint64_t*
    public static native long resultNullmaskData(long resultPtr, long col);
    // return zero terminated utf8 string or 0
    public static native long resultError(long resultPtr);
    // return data_chunk pointer
    // must be destroyed with dataChunkDestroy
    public static native long resultGetDataChunk(long resultPtr, long chunkIndex);
    public static native long resultDataChunkCount(long resultPtr);

    public static native void dataChunkDestroy(long chunkPtr);
    public static native long dataChunkGetColumnCount(long chunkPtr);
    public static native long dataChunkGetVector(long chunkPtr, long colIdx);
    public static native long dataChunkGetSize(long chunkPtr);

    // return physical type not logical, could be updated later
    public static native long vectorGetColumnType(long vectorPtr);
    public static native long vectorGetData(long vectorPtr);
    // return uint64_t*
    // use validityRowIsValid to check if row is valid
    public static native long vectorGetValidity(long vectorPtr);
}
