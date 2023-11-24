#ifndef QUESTDB_DUCKDB_JNI_H
#define QUESTDB_DUCKDB_JNI_H

#include <jni.h>

extern "C" {
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpen(JNIEnv*, jclass, jlong path_ptr, jlong path_size);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpenExt(JNIEnv*, jclass, jlong path_ptr, jlong path_size, jlong config_ptr);
    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_databaseClose(JNIEnv*, jclass, jlong db);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseConnect(JNIEnv*, jclass, jlong db);

    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionInterrupt(JNIEnv*, jclass, jlong connection);
    JNIEXPORT jdouble JNICALL Java_io_questdb_duckdb_DuckDB_connectionQueryProgress(JNIEnv*, jclass, jlong connection);
    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionDisconnect(JNIEnv*, jclass, jlong connection);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionQuery(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionExec(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size);

    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configCreate(JNIEnv*, jclass);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configSet(JNIEnv*, jclass, jlong config, jlong name_ptr, jlong name_size, jlong option_ptr, jlong option_size);
    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_configDestroy(JNIEnv*, jclass, jlong config);

    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_resultDestroy(JNIEnv*, jclass, jlong result);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnName(JNIEnv*, jclass, jlong result, jlong col);
    JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnType(JNIEnv*, jclass, jlong result, jlong col);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnCount(JNIEnv*, jclass, jlong result);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultRowCount(JNIEnv*, jclass, jlong result);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnData(JNIEnv*, jclass, jlong result, jlong col);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultNullmaskData(JNIEnv*, jclass, jlong result, jlong col);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultError(JNIEnv*, jclass, jlong result);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetDataChunk(JNIEnv*, jclass, jlong result, jlong chunk_index);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultDataChunkCount(JNIEnv*, jclass, jlong result);

    JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkDestroy(JNIEnv*, jclass, jlong chunk);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetColumnCount(JNIEnv*, jclass, jlong chunk);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetVector(JNIEnv*, jclass, jlong chunk, jlong col_idx);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetSize(JNIEnv*, jclass, jlong chunk);

    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetColumnType(JNIEnv*, jclass, jlong vector);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetData(JNIEnv*, jclass, jlong vector);
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetValidity(JNIEnv*, jclass, jlong vector);
}

#endif //QUESTDB_DUCKDB_JNI_H
