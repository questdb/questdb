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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

public class DuckDB {
    public static final int DUCKDB_TYPE_INVALID = 0;
    public static final int DUCKDB_TYPE_SQLNULL = 1; /* NULL type, used for constant NULL */
    public static final int DUCKDB_TYPE_UNKNOWN = 2; /* unknown type, used for parameter expressions */
    public static final int DUCKDB_TYPE_ANY = 3;     /* ANY type, used for functions that accept any type as parameter */
    public static final int DUCKDB_TYPE_USER = 4; /* A User Defined Type (e.g., ENUMs before the binder) */
    public static final int DUCKDB_TYPE_BOOLEAN = 10;
    public static final int DUCKDB_TYPE_TINYINT = 11;
    public static final int DUCKDB_TYPE_SMALLINT = 12;
    public static final int DUCKDB_TYPE_INTEGER = 13;
    public static final int DUCKDB_TYPE_BIGINT = 14;
    public static final int DUCKDB_TYPE_DATE = 15;
    public static final int DUCKDB_TYPE_TIME = 16;
    public static final int DUCKDB_TYPE_TIMESTAMP_SEC = 17;
    public static final int DUCKDB_TYPE_TIMESTAMP_MS = 18;
    public static final int DUCKDB_TYPE_TIMESTAMP = 19; //! us
    public static final int DUCKDB_TYPE_TIMESTAMP_NS = 20;
    public static final int DUCKDB_TYPE_DECIMAL = 21;
    public static final int DUCKDB_TYPE_FLOAT = 22;
    public static final int DUCKDB_TYPE_DOUBLE = 23;
    public static final int DUCKDB_TYPE_CHAR = 24;
    public static final int DUCKDB_TYPE_VARCHAR = 25;
    public static final int DUCKDB_TYPE_BLOB = 26;
    public static final int DUCKDB_TYPE_INTERVAL = 27;
    public static final int DUCKDB_TYPE_UTINYINT = 28;
    public static final int DUCKDB_TYPE_USMALLINT = 29;
    public static final int DUCKDB_TYPE_UINTEGER = 30;
    public static final int DUCKDB_TYPE_UBIGINT = 31;
    public static final int DUCKDB_TYPE_TIMESTAMP_TZ = 32;
    public static final int DUCKDB_TYPE_TIME_TZ = 34;
    public static final int DUCKDB_TYPE_JSON = 35;

    public static final int DUCKDB_TYPE_HUGEINT = 50;
    public static final int DUCKDB_TYPE_POINTER = 51;
    public static final int DUCKDB_TYPE_VALIDITY = 53;
    public static final int DUCKDB_TYPE_UUID = 54;

    public static final int DUCKDB_TYPE_STRUCT = 100;
    public static final int DUCKDB_TYPE_LIST = 101;
    public static final int DUCKDB_TYPE_MAP = 102;
    public static final int DUCKDB_TYPE_TABLE = 103;
    public static final int DUCKDB_TYPE_ENUM = 104;
    public static final int DUCKDB_TYPE_AGGREGATE_STATE = 105;
    public static final int DUCKDB_TYPE_LAMBDA = 106;
    public static final int DUCKDB_TYPE_UNION = 107;

    public static int getQdbColumnType(int duckDbType) {
        switch (duckDbType) {
            case DuckDB.DUCKDB_TYPE_BOOLEAN:
                return ColumnType.BOOLEAN;
            case DuckDB.DUCKDB_TYPE_TINYINT:
                return ColumnType.BYTE;
            case DuckDB.DUCKDB_TYPE_SMALLINT:
                return ColumnType.SHORT;
            case DuckDB.DUCKDB_TYPE_INTEGER:
                return ColumnType.INT;
            case DuckDB.DUCKDB_TYPE_BIGINT:
                return ColumnType.LONG;
            case DuckDB.DUCKDB_TYPE_FLOAT:
                return ColumnType.FLOAT;
            case DuckDB.DUCKDB_TYPE_DOUBLE:
                return ColumnType.DOUBLE;
            case DuckDB.DUCKDB_TYPE_VARCHAR:
                return ColumnType.STRING;
            case DuckDB.DUCKDB_TYPE_DATE:
                return ColumnType.DATE;
            case DuckDB.DUCKDB_TYPE_TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case DuckDB.DUCKDB_TYPE_HUGEINT:
                return ColumnType.UUID;
            default:
                return ColumnType.UNDEFINED;
        }
    }

    public static boolean validityRowIsValid(long validityPtr, long row) {
        if (validityPtr == 0) {
            return true;
        }
        long entry_idx = row / 64;
        long idx_in_entry = row % 64;
        long mask = Unsafe.getUnsafe().getLong(validityPtr + entry_idx * 8);
        return (mask & (1L << idx_in_entry)) != 0;
    }

    // Database API
    public static native long databaseOpen(long pathPtr, long pathSize);
    public static native long databaseOpenExt(long pathPtr, long pathSize, long configPtr);
    public static native void databaseClose(long dbPtr);
    public static native long databaseConnect(long dbPtr);

    // Connection API
    public static native void connectionInterrupt(long connectionPtr);
    public static native void connectionDisconnect(long connectionPrt);
    public static native long connectionQuery(long connectionPtr, long queryPtr, long querySize);

    // Configuration API
    public static native long configCreate();
    public static native boolean configSet(long configPtr, long namePtr, long nameSize, long optionPtr, long optionSize);
    public static native void configDestroy(long configPtr);

    // Prepared Statement API
    public static native long connectionPrepare(long connection, long query_ptr, long query_size);
    public static native long preparedExecute(long stmt);
    public static native void preparedDestroy(long stmt);
    public static native long preparedGetError(long stmt);
    public static native long preparedGetQueryText(long stmt);
    public static native int preparedGetStatementType(long stmt);
    public static native int preparedGetStatementReturnType(long stmt);
    public static native boolean preparedAllowStreamResult(long stmt);
    public static native long preparedParameterCount(long stmt);
    public static native long preparedGetColumnCount(long stmt);
    public static native int preparedGetColumnLogicalType(long stmt, long col);
    public static native int preparedGetColumnPhysicalType(long stmt, long col);
    public static native long preparedGetColumnName(long stmt, long col);

    // Result API
    public static native long resultGetError(long result);
    public static native long resultFetchChunk(long result);
    public static native void resultDestroy(long resultPtr);
    public static native long resultColumnName(long resultPtr, long col);
    public static native int resultColumnPhysicalType(long resultPtr, long col);
    public static native int resultColumnLogicalType(long resultPtr, long col);
    public static native long resultColumnCount(long resultPtr);
    public static native long resultGetMaterialized(long resultPtr);
    public static native long resultRowCount(long resultPtr);
    public static native long resultError(long resultPtr);
    public static native int resultGetQueryResultType(long resultPtr);
    public static native long resultGetDataChunk(long resultPtr, long chunkIndex);
    public static native long resultDataChunkCount(long resultPtr);

    // Data Chunk API
    public static native void dataChunkDestroy(long chunkPtr);
    public static native long dataChunkGetColumnCount(long chunkPtr);
    public static native long dataChunkGetVector(long chunkPtr, long colIdx);
    public static native long dataChunkGetSize(long chunkPtr);

    // Vector API
    public static native int vectorGetColumnLogicalType(long vectorPtr);
    public static native int vectorGetColumnPhysicalType(long vectorPtr);
    public static native long vectorGetData(long vectorPtr);
    public static native long vectorGetValidity(long vectorPtr);
}
