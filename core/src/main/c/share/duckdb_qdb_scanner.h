#ifndef QUESTDB_DUCKDB_QDB_SCANNER_H
#define QUESTDB_DUCKDB_QDB_SCANNER_H

#include <jni.h>

extern "C" {
    JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_registerQuestDBScanFunction(JNIEnv *, jclass, jlong);
}

#endif //QUESTDB_DUCKDB_QDB_SCANNER_H
