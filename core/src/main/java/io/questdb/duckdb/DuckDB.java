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

// This API is highly unsafe and should be used with caution.
// JNI calls are not checked for errors and memory is not freed automatically.
// The caller is responsible for freeing all memory allocated by the API.
// The caller is responsible for checking for validity the arguments of all API calls.
// C++ native exceptions are converted to thread local DedicatedError value.
// The caller is responsible for checking the error value with errorType() and errorMessage().
// Prepared Statements and Query Results can be constructed from error values.
// The caller is responsible for checking the Statement/Result error value with statementGetError/resultGetError.

public class DuckDB {
    public static final int ERROR_TYPE_INVALID = 0;          // invalid type
    public static final int ERROR_TYPE_OUT_OF_RANGE = 1;     // value out of range error
    public static final int ERROR_TYPE_CONVERSION = 2;       // conversion/casting error
    public static final int ERROR_TYPE_UNKNOWN_TYPE = 3;     // unknown type
    public static final int ERROR_TYPE_DECIMAL = 4;          // decimal related
    public static final int ERROR_TYPE_MISMATCH_TYPE = 5;    // type mismatch
    public static final int ERROR_TYPE_DIVIDE_BY_ZERO = 6;   // divide by 0
    public static final int ERROR_TYPE_OBJECT_SIZE = 7;      // object size exceeded
    public static final int ERROR_TYPE_INVALID_TYPE = 8;     // incompatible for operation
    public static final int ERROR_TYPE_SERIALIZATION = 9;    // serialization
    public static final int ERROR_TYPE_TRANSACTION = 10;     // transaction management
    public static final int ERROR_TYPE_NOT_IMPLEMENTED = 11; // method not implemented
    public static final int ERROR_TYPE_EXPRESSION = 12;      // expression parsing
    public static final int ERROR_TYPE_CATALOG = 13;         // catalog related
    public static final int ERROR_TYPE_PARSER = 14;          // parser related
    public static final int ERROR_TYPE_PLANNER = 15;         // planner related
    public static final int ERROR_TYPE_SCHEDULER = 16;       // scheduler related
    public static final int ERROR_TYPE_EXECUTOR = 17;        // executor related
    public static final int ERROR_TYPE_CONSTRAINT = 18;      // constraint related
    public static final int ERROR_TYPE_INDEX = 19;           // index related
    public static final int ERROR_TYPE_STAT = 20;            // stat related
    public static final int ERROR_TYPE_CONNECTION = 21;      // connection related
    public static final int ERROR_TYPE_SYNTAX = 22;          // syntax related
    public static final int ERROR_TYPE_SETTINGS = 23;        // settings related
    public static final int ERROR_TYPE_BINDER = 24;          // binder related
    public static final int ERROR_TYPE_NETWORK = 25;         // network related
    public static final int ERROR_TYPE_OPTIMIZER = 26;       // optimizer related
    public static final int ERROR_TYPE_NULL_POINTER = 27;    // nullptr exception
    public static final int ERROR_TYPE_IO = 28;              // IO exception
    public static final int ERROR_TYPE_INTERRUPT = 29;       // interrupt
    public static final int ERROR_TYPE_FATAL = 30;           // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
    public static final int ERROR_TYPE_INTERNAL = 31;        // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
    public static final int ERROR_TYPE_INVALID_INPUT = 32;   // Input or arguments error
    public static final int ERROR_TYPE_OUT_OF_MEMORY = 33;   // out of memory
    public static final int ERROR_TYPE_PERMISSION = 34;      // insufficient permissions
    public static final int ERROR_TYPE_PARAMETER_NOT_RESOLVED = 35; // parameter types could not be resolved
    public static final int ERROR_TYPE_PARAMETER_NOT_ALLOWED = 36;  // parameter types not allowed
    public static final int ERROR_TYPE_DEPENDENCY = 37;             // dependency

	public static final int STMT_TYPE_INVALID_STATEMENT = 0;      // invalid statement type
	public static final int STMT_TYPE_SELECT_STATEMENT = 1;       // select statement type
	public static final int STMT_TYPE_INSERT_STATEMENT = 2;       // insert statement type
	public static final int STMT_TYPE_UPDATE_STATEMENT = 3;       // update statement type
	public static final int STMT_TYPE_CREATE_STATEMENT = 4;       // create statement type
	public static final int STMT_TYPE_DELETE_STATEMENT = 5;       // delete statement type
	public static final int STMT_TYPE_PREPARE_STATEMENT = 6;      // prepare statement type
	public static final int STMT_TYPE_EXECUTE_STATEMENT = 7;      // execute statement type
	public static final int STMT_TYPE_ALTER_STATEMENT = 8;        // alter statement type
	public static final int STMT_TYPE_TRANSACTION_STATEMENT = 9;  // transaction statement type,
	public static final int STMT_TYPE_COPY_STATEMENT = 10;         // copy type
	public static final int STMT_TYPE_ANALYZE_STATEMENT = 11;      // analyze type
	public static final int STMT_TYPE_VARIABLE_SET_STATEMENT = 12; // variable set statement type
	public static final int STMT_TYPE_CREATE_FUNC_STATEMENT = 13;  // create func statement type
	public static final int STMT_TYPE_EXPLAIN_STATEMENT = 14;      // explain statement type
	public static final int STMT_TYPE_DROP_STATEMENT = 15;         // DROP statement type
	public static final int STMT_TYPE_EXPORT_STATEMENT = 16;       // EXPORT statement type
	public static final int STMT_TYPE_PRAGMA_STATEMENT = 17;       // PRAGMA statement type
	public static final int STMT_TYPE_SHOW_STATEMENT = 18;         // SHOW statement type
	public static final int STMT_TYPE_VACUUM_STATEMENT = 19;       // VACUUM statement type
	public static final int STMT_TYPE_CALL_STATEMENT = 20;         // CALL statement type
	public static final int STMT_TYPE_SET_STATEMENT = 21;          // SET statement type
	public static final int STMT_TYPE_LOAD_STATEMENT = 22;         // LOAD statement type
	public static final int STMT_TYPE_RELATION_STATEMENT = 23;
	public static final int STMT_TYPE_EXTENSION_STATEMENT = 24;
	public static final int STMT_TYPE_LOGICAL_PLAN_STATEMENT = 25;

    public static final int STMT_RETURN_TYPE_QUERY_RESULT = 0; // the statement returns a query result (e.g. for display to the user)
    public static final int STMT_RETURN_TYPE_CHANGED_ROWS = 1; // the statement returns a single row containing the number of changed rows (e.g. an insert stmt)
    public static final int STMT_RETURN_TYPE_NOTHING = 2;      // the statement returns nothing

    public static final int QUERY_MATERIALIZED_RESULT = 0;
    public static final int QUERY_STREAM_RESULT = 1;
    public static final int QUERY_PENDING_RESULT = 2;

    public static final int COLUMN_TYPE_INVALID = 0;
    public static final int COLUMN_TYPE_SQLNULL = 1; /* NULL type, used for constant NULL */
    public static final int COLUMN_TYPE_UNKNOWN = 2; /* unknown type, used for parameter expressions */
    public static final int COLUMN_TYPE_ANY = 3;     /* ANY type, used for functions that accept any type as parameter */
    public static final int COLUMN_TYPE_USER = 4; /* A User Defined Type (e.g., ENUMs before the binder) */
    public static final int COLUMN_TYPE_BOOLEAN = 10;
    public static final int COLUMN_TYPE_TINYINT = 11;
    public static final int COLUMN_TYPE_SMALLINT = 12;
    public static final int COLUMN_TYPE_INTEGER = 13;
    public static final int COLUMN_TYPE_BIGINT = 14;
    public static final int COLUMN_TYPE_DATE = 15;
    public static final int COLUMN_TYPE_TIME = 16;
    public static final int COLUMN_TYPE_TIMESTAMP_SEC = 17;
    public static final int COLUMN_TYPE_TIMESTAMP_MS = 18;
    public static final int COLUMN_TYPE_TIMESTAMP = 19; //! us
    public static final int COLUMN_TYPE_TIMESTAMP_NS = 20;
    public static final int COLUMN_TYPE_DECIMAL = 21;
    public static final int COLUMN_TYPE_FLOAT = 22;
    public static final int COLUMN_TYPE_DOUBLE = 23;
    public static final int COLUMN_TYPE_CHAR = 24;
    public static final int COLUMN_TYPE_VARCHAR = 25;
    public static final int COLUMN_TYPE_BLOB = 26;
    public static final int COLUMN_TYPE_INTERVAL = 27;
    public static final int COLUMN_TYPE_UTINYINT = 28;
    public static final int COLUMN_TYPE_USMALLINT = 29;
    public static final int COLUMN_TYPE_UINTEGER = 30;
    public static final int COLUMN_TYPE_UBIGINT = 31;
    public static final int COLUMN_TYPE_TIMESTAMP_TZ = 32;
    public static final int COLUMN_TYPE_TIME_TZ = 34;
    public static final int COLUMN_TYPE_JSON = 35;
    public static final int COLUMN_TYPE_HUGEINT = 50;
    public static final int COLUMN_TYPE_POINTER = 51;
    public static final int COLUMN_TYPE_VALIDITY = 53;
    public static final int COLUMN_TYPE_UUID = 54;
    public static final int COLUMN_TYPE_STRUCT = 100;
    public static final int COLUMN_TYPE_LIST = 101;
    public static final int COLUMN_TYPE_MAP = 102;
    public static final int COLUMN_TYPE_TABLE = 103;
    public static final int COLUMN_TYPE_ENUM = 104;
    public static final int COLUMN_TYPE_AGGREGATE_STATE = 105;
    public static final int COLUMN_TYPE_LAMBDA = 106;
    public static final int COLUMN_TYPE_UNION = 107;

    // Error API
    // returns the error type of the last error that occurred
    public static native int errorType();
    // returns the error message (const char*) of the last error that occurred
    public static native long errorMessage();
    // Database API
    // returns a new DuckDB database instance ptr
    public static native long databaseOpen(long path_ptr, long path_size);
    // returns a new DuckDB database instance ptr
    public static native long databaseOpenExt(long path_ptr, long path_size, long config_ptr);
    public static native void databaseClose(long db);
    // returns a new DuckDB connection instance ptr
    // connectionDisconnect must be called to free the connection
    public static native long databaseConnect(long db);
    // Connection API
    public static native void connectionInterrupt(long connection);
    public static native void connectionDisconnect(long connection);
    public static native long connectionQuery(long connection, long query_ptr, long query_size);
    // Configuration API
    // returns a new DuckDB config instance ptr
    // configDestroy must be called to free the config
    public static native long configCreate();
    // returns 1 on success, 0 on name not found, -1 on failure
    public static native int configSet(long config, long name_ptr, long name_size, long option_ptr, long option_size);
    public static native void configDestroy(long config);
    // Prepared Statement API
    // returns a new DuckDB prepared statement instance ptr
    // preparedDestroy must be called to free the prepared statement
    public static native long connectionPrepare(long connection, long query_ptr, long query_size);
    public static native long preparedExecute(long stmt);
    public static native void preparedDestroy(long stmt);
    public static native long preparedGetError(long stmt);
    public static native long preparedGetQueryText(long stmt);
    // returns prepared statement properties long encoded as follows:
    // bits 0-7:   statement type
    // bits 8-15:  return type
    // bits 16-31: parameter count
    // bit 32: read only flag
    // bit 33: requires_valid_transaction flag
    // bit 34: allow_stream_result flag
    // bit 35: bound_all_parameters flag
    public static native long preparedGetStatementProperties(long stmt);
    public static native long preparedGetColumnCount(long stmt);
    // returns column types (PhysicalTypeId, LogicalTypeId) encoded as long
    // bits 0-31:  LogicalTypeId
    // bits 32-63: PhysicalTypeId
    public static native long preparedGetColumnTypes(long stmt, long col);
    // returns column names (const char*) encoded as long
    public static native long preparedGetColumnName(long stmt, long col);
    // Result API
    // returns a new DuckDB result error (const char*) encoded as long
    // 0 if no error occurred
    public static native long resultGetError(long result);
    // returns a new DuckDB chunk instance ptr
    // dataChunkDestroy must be called to free the chunk
    public static native long resultFetchChunk(long result);
    public static native void resultDestroy(long result);
    public static native long resultColumnName(long result, long col);
    // returns column types (PhysicalTypeId, LogicalTypeId) encoded as long
    public static native long resultColumnTypes(long result, long col);
    public static native long resultColumnCount(long result);
    // creates a new materialized result from the given result
    public static native long resultGetMaterialized(long result);
    public static native long resultRowCount(long result);
    // returns the query result type (see QUERY_*_RESULT constants)
    public static native int resultGetQueryResultType(long result);
    // can be called only on materialized results
    public static native long resultGetDataChunk(long result, long chunk_index);
    // can be called only on materialized results
    public static native long resultDataChunkCount(long result);
    // Data Chunk API
    public static native void dataChunkDestroy(long chunk);
    public static native long dataChunkGetColumnCount(long chunk);
    // returns the vector of the specified column (void*)
    public static native long dataChunkGetVector(long chunk, long col_idx);
    // returns the number of rows in the chunk
    public static native long dataChunkGetSize(long chunk);
    // Vector API
    // returns the vector type (PhysicalTypeId, LogicalTypeId) encoded as long
    public static native long vectorGetColumnTypes(long vector);
    // returns the vector data (void*)
    public static native long vectorGetData(long vector);
    // returns the validity mask of the vector (uint64_t*)
    public static native long vectorGetValidity(long vector);

    public static int decodeLogicalTypeId(long encoded) {
        return (int) (encoded & 0xFFFFFFFFL);
    }

    public static int decodePhysicalTypeId(long encoded) {
        return (int) (encoded >> 32);
    }

    public static int decodeStatementType(long encoded) {
        return (int) (encoded & 0xFF);
    }

    public static int decodeStatementReturnType(long encoded) {
        return (int) ((encoded >> 8) & 0xFF);
    }

    public static int decodeStatementParameterCount(long encoded) {
        return (int) ((encoded >> 16) & 0xFFFF);
    }

    public static boolean decodeStatementReadOnly(long encoded) {
        return (encoded & (1L << 32)) != 0;
    }

    public static boolean decodeStatementRequiresValidTransaction(long encoded) {
        return (encoded & (1L << 33)) != 0;
    }

    public static boolean decodeStatementAllowStreamResult(long encoded) {
        return (encoded & (1L << 34)) != 0;
    }

    public static boolean decodeStatementBoundAllParameters(long encoded) {
        return (encoded & (1L << 35)) != 0;
    }

    public static int getQdbColumnType(int duckDbType) {
        switch (duckDbType) {
            case DuckDB.COLUMN_TYPE_BOOLEAN:
                return ColumnType.BOOLEAN;
            case DuckDB.COLUMN_TYPE_TINYINT:
                return ColumnType.BYTE;
            case DuckDB.COLUMN_TYPE_SMALLINT:
                return ColumnType.SHORT;
            case DuckDB.COLUMN_TYPE_INTEGER:
                return ColumnType.INT;
            case DuckDB.COLUMN_TYPE_BIGINT:
                return ColumnType.LONG;
            case DuckDB.COLUMN_TYPE_FLOAT:
                return ColumnType.FLOAT;
            case DuckDB.COLUMN_TYPE_DOUBLE:
                return ColumnType.DOUBLE;
            case DuckDB.COLUMN_TYPE_VARCHAR:
                return ColumnType.STRING;
            case DuckDB.COLUMN_TYPE_DATE:
                return ColumnType.DATE;
            case DuckDB.COLUMN_TYPE_TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case DuckDB.COLUMN_TYPE_HUGEINT:
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
}
