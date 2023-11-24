//
// Created by Ievgenii Lysiuchenko on 23.11.2023.
//

#include <string>
#include "duckdb_jni.h"
#include <duckdb.h>
#include <iostream>


//JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_duckDB(JNIEnv *e, jclass cl) {
//    using namespace duckdb;
//    DuckDB db(nullptr);
//
//    Connection con(db);
//
////    con.Query("CREATE TABLE integers(i INTEGER)");
////    con.Query("INSERT INTO integers VALUES (3)");
////    con.Query("INSERT INTO integers VALUES (6)");
////    con.Query("INSERT INTO integers VALUES (9)");
////    auto result = con.Query("SELECT * FROM integers");
//    con.Query("INSTALL parquet;");
//    con.Query("LOAD parquet;");
//    auto result = con.Query("SELECT * FROM read_parquet('/Users/eugene/Dev/duckdb_data/data/parquet-testing/simple.parquet')");
//    result->Print();
//
//    return 0;
//}


// Open/Connect/Close

// duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpen(JNIEnv*, jclass, jlong path_ptr, jlong path_size) {
    duckdb_database db = nullptr;
    std::string path((const char *) path_ptr, path_size);
    duckdb_state state = duckdb_open(path.c_str(), &db);
    if (state != DuckDBSuccess) {
        return 0;
    }
    return (jlong) db;
}

// duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config_ptr, char **out_error);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpenExt(JNIEnv*, jclass, jlong path_ptr, jlong path_size, jlong config_ptr) {
    duckdb_database db = nullptr;
    std::string path((const char *) path_ptr, path_size);
    duckdb_state state = duckdb_open_ext(path.c_str(), &db, (duckdb_config) config_ptr, nullptr); // TODO: error reporting
    if (state != DuckDBSuccess) {
        return 0;
    }
    return (jlong) db;
}

// void duckdb_close(duckdb_database *database);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_databaseClose(JNIEnv*, jclass, jlong db) {
    auto database = (duckdb_database) db;
    duckdb_close(&database);
}

// duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseConnect(JNIEnv*, jclass, jlong db) {
    duckdb_connection connection = nullptr;
    duckdb_state state = duckdb_connect((duckdb_database) db, &connection);
    if (state != DuckDBSuccess) {
        return 0;
    }
    return (jlong) connection;
}

// void duckdb_interrupt(duckdb_connection connection);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionInterrupt(JNIEnv*, jclass, jlong connection) {
    duckdb_interrupt((duckdb_connection) connection);
}

// double duckdb_query_progress(duckdb_connection connection);
JNIEXPORT jdouble JNICALL Java_io_questdb_duckdb_DuckDB_connectionQueryProgress(JNIEnv*, jclass, jlong connection) {
    return duckdb_query_progress((duckdb_connection) connection);
}

// void duckdb_disconnect(duckdb_connection *connection);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionDisconnect(JNIEnv*, jclass, jlong connection) {
    auto conn = (duckdb_connection) connection;
    duckdb_disconnect(&conn);
}

// Configuration

// duckdb_state duckdb_create_config(duckdb_config *out_config);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configCreate(JNIEnv*, jclass) {
    duckdb_config config = nullptr;
    duckdb_state state = duckdb_create_config(&config);
    if (state != DuckDBSuccess) {
        return 0;
    }
    return (jlong) config;
}

// duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configSet(JNIEnv*, jclass, jlong config, jlong name_ptr, jlong name_size, jlong option_ptr, jlong option_size) {
    std::string name((const char *) name_ptr, name_size);
    std::string option((const char *) option_ptr, option_size);
    duckdb_state state = duckdb_set_config((duckdb_config) config, name.c_str(), option.c_str());
    return state;
}

// void duckdb_destroy_config(duckdb_config *config);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_configDestroy(JNIEnv*, jclass, jlong config) {
    auto cfg = (duckdb_config) config;
    duckdb_destroy_config(&cfg);
}

// Query Execution

// duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionQuery(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size) {
    std::string query((const char *) query_ptr, query_size);
    auto* result = (duckdb_result *) malloc(sizeof(duckdb_result));
    duckdb_state state = duckdb_query((duckdb_connection) connection, query.c_str(), result);
    if (state != DuckDBSuccess) {
        return 0;
    }
    return (jlong) result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionExec(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size) {
    std::string query((const char *) query_ptr, query_size);
    return duckdb_query((duckdb_connection) connection, query.c_str(), nullptr);
}

// void duckdb_destroy_result(duckdb_result *result);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_resultDestroy(JNIEnv*, jclass, jlong result) {
    auto *res = (duckdb_result *) result;
    duckdb_destroy_result(res);
    free(res);
}

// const char *duckdb_column_name(duckdb_result *result, idx_t col);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnName(JNIEnv*, jclass, jlong result, jlong col) {
    return (jlong) duckdb_column_name((duckdb_result *) result, (idx_t) col);
}

// duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);
JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnType(JNIEnv*, jclass, jlong result, jlong col) {
    return (jint) duckdb_column_type((duckdb_result *) result, (idx_t) col);
}

// idx_t duckdb_column_count(duckdb_result *result);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnCount(JNIEnv*, jclass, jlong result) {
    return (jlong) duckdb_column_count((duckdb_result *) result);
}

// idx_t duckdb_row_count(duckdb_result *result);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultRowCount(JNIEnv*, jclass, jlong result) {
    return (jlong) duckdb_row_count((duckdb_result *) result);
}

// void *duckdb_column_data(duckdb_result *result, idx_t col);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnData(JNIEnv*, jclass, jlong result, jlong col) {
    return (jlong) duckdb_column_data((duckdb_result *) result, (idx_t) col);
}

// bool *duckdb_nullmask_data(duckdb_result *result, idx_t col);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultNullmaskData(JNIEnv*, jclass, jlong result, jlong col) {
    return (jlong) duckdb_nullmask_data((duckdb_result *) result, (idx_t) col);
}

// const char *duckdb_result_error(duckdb_result *result);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultError(JNIEnv*, jclass, jlong result) {
    return (jlong) duckdb_result_error((duckdb_result *) result);
}

// Result Helpers

// duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetDataChunk(JNIEnv*, jclass, jlong result, jlong chunk_index) {
    return (jlong) duckdb_result_get_chunk(*(duckdb_result *) result, (idx_t) chunk_index);
}

// void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);
JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkDestroy(JNIEnv*, jclass, jlong chunk) {
    auto data_chunk = (duckdb_data_chunk) chunk;
    duckdb_destroy_data_chunk(&data_chunk);
}

// idx_t duckdb_result_chunk_count(duckdb_result result);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultDataChunkCount(JNIEnv*, jclass, jlong result) {
    return (jlong) duckdb_result_chunk_count(*(duckdb_result *) result);
}

// Data Chunk

// idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetColumnCount(JNIEnv*, jclass, jlong chunk) {
    return (jlong) duckdb_data_chunk_get_column_count((duckdb_data_chunk) chunk);
}

// duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetVector(JNIEnv*, jclass, jlong chunk, jlong col_idx) {
    return (jlong) duckdb_data_chunk_get_vector((duckdb_data_chunk) chunk, (idx_t) col_idx);
}

// idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetSize(JNIEnv*, jclass, jlong chunk) {
    return (jlong) duckdb_data_chunk_get_size((duckdb_data_chunk) chunk);
}

// Vector

// duckdb_type duckdb_vector_get_column_type(duckdb_vector vector);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetColumnType(JNIEnv*, jclass, jlong vector) {
    return (jlong) duckdb_get_type_id(duckdb_vector_get_column_type((duckdb_vector) vector));
}

// void *duckdb_vector_get_data(duckdb_vector vector);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetData(JNIEnv*, jclass, jlong vector) {
    return (jlong) duckdb_vector_get_data((duckdb_vector) vector);
}

// uint64_t *duckdb_vector_get_validity(duckdb_vector vector);
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetValidity(JNIEnv*, jclass, jlong vector) {
    return (jlong) duckdb_vector_get_validity((duckdb_vector) vector);
}