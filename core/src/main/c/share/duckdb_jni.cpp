#include <memory>
#include <string>
#include "duckdb_jni.h"
#include <duckdb.hpp>
//#include <malloc_extension.h>
//#include <iostream>
//
//static size_t CurrentlyAllocatedBytes() {
//  size_t value;
//  MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &value);
//  return value;
//}

thread_local duckdb::PreservedError duckdb_error;

uint64_t pack_duckdb_types(duckdb::PhysicalType physical_type, duckdb::LogicalTypeId logical_type) {
    return (uint64_t) physical_type << 32 | (uint64_t) logical_type;
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_errorType(JNIEnv *, jclass) {
    return duckdb_error ? (jint) duckdb_error.Type() : -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_errorMessage(JNIEnv *, jclass) {
    return duckdb_error ? (jlong) duckdb_error.Message().c_str() : 0;
}

// Database API

JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_databaseOpen(JNIEnv *env, jclass cs, jlong path_ptr, jlong path_size) {
    return Java_io_questdb_duckdb_DuckDB_databaseOpenExt(env, cs, path_ptr, path_size, 0);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_databaseOpenExt(JNIEnv *, jclass, jlong path_ptr, jlong path_size, jlong config_ptr) {
    try {
        std::string path((const char *) path_ptr, path_size);
        return (jlong) new duckdb::DuckDB(path.c_str(), (duckdb::DBConfig *) config_ptr);
    } catch (const duckdb::Exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (const std::exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (...) {
        duckdb_error = duckdb::PreservedError("Unknown error in databaseOpenExt");
    }
    return (jlong) nullptr;
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_databaseClose(JNIEnv *, jclass, jlong db) {
            D_ASSERT(db > 0);
    delete (duckdb::DuckDB *) db;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseConnect(JNIEnv *, jclass, jlong db) {
            D_ASSERT(db > 0);
    try {
//        auto a = CurrentlyAllocatedBytes();
        jlong ptr = (jlong) new duckdb::Connection(*(duckdb::DuckDB *) db);
//        auto b = CurrentlyAllocatedBytes();
//        std::cerr << "Allocated " << (b - a) << " bytes for connection" << std::endl;
        return ptr;
    } catch (const duckdb::Exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (const std::exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (...) {
        duckdb_error = duckdb::PreservedError("Unknown error in databaseConnect");
    }
    return (jlong) nullptr;
}

// Connection API

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionInterrupt(JNIEnv *, jclass, jlong connection) {
            D_ASSERT(connection > 0);
    ((duckdb::Connection *) connection)->Interrupt();
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionDisconnect(JNIEnv *, jclass, jlong connection) {
            D_ASSERT(connection > 0);
    delete (duckdb::Connection *) connection;
}

// returns materialized result or PreservedError wrapped in
JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_connectionQuery(JNIEnv *, jclass, jlong connection, jlong query_ptr, jlong query_size) {
            D_ASSERT(connection > 0);
            D_ASSERT(query_ptr > 0);
            D_ASSERT(query_size > 0);

    std::string query((const char *) query_ptr, query_size);
    return (jlong) ((duckdb::Connection *) connection)->Query(query).release();
}

// Configuration API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configCreate(JNIEnv *, jclass) {
    try {
        return (jlong) new duckdb::DBConfig();
    } catch (const duckdb::Exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (const std::exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (...) {
        duckdb_error = duckdb::PreservedError("Unknown error in configCreate");
    }
    return (jlong) nullptr;
}

JNIEXPORT jint JNICALL
Java_io_questdb_duckdb_DuckDB_configSet(JNIEnv *, jclass, jlong config, jlong name_ptr, jlong name_size,
                                        jlong option_ptr, jlong option_size) {

            D_ASSERT(config > 0);
            D_ASSERT(name_ptr > 0);
            D_ASSERT(name_size > 0);
            D_ASSERT(option_ptr > 0);
            D_ASSERT(option_size > 0);

    std::string name((const char *) name_ptr, name_size);
    std::string option((const char *) option_ptr, option_size);

    auto cfg_option = duckdb::DBConfig::GetOptionByName(name);
    if (!cfg_option) {
        return 0;
    }
    try {
        ((duckdb::DBConfig *) config)->SetOption(*cfg_option, duckdb::Value(option));
        return 1;
    } catch (const duckdb::Exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (const std::exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (...) {
        duckdb_error = duckdb::PreservedError("Unknown error in configSet");
    }
    return -1;
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_configDestroy(JNIEnv *, jclass, jlong config) {
            D_ASSERT(config > 0);
    delete (duckdb::DBConfig *) config;
}

// Prepared Statement API

JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_connectionPrepare(JNIEnv *, jclass, jlong connection, jlong query_ptr, jlong query_size) {
            D_ASSERT(connection > 0);
            D_ASSERT(query_ptr > 0);
            D_ASSERT(query_size > 0);
    std::string query((const char *) query_ptr, query_size);
//    auto a = CurrentlyAllocatedBytes();
    jlong ptr = (jlong) ((duckdb::Connection *) connection)->Prepare(query).release();
//    auto b = CurrentlyAllocatedBytes();
//    std::cerr << "Allocated " << (b - a) << " bytes for prepared statement: " << query << std::endl;
    return ptr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedExecute(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    try {
        return (jlong) ((duckdb::PreparedStatement *) stmt)->Execute().release();
    } catch (const duckdb::Exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (const std::exception &ex) {
        duckdb_error = duckdb::PreservedError(ex);
    } catch (...) {
        duckdb_error = duckdb::PreservedError("Unknown error in preparedExecute");
    }
    return (jlong) nullptr;
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_preparedDestroy(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    delete (duckdb::PreparedStatement *) stmt;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetError(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    auto statement = (duckdb::PreparedStatement *) stmt;
    return statement->HasError() ? (jlong) statement->GetError().c_str() : (jlong) nullptr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetQueryText(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    return (jlong) ((duckdb::PreparedStatement *) stmt)->query.c_str();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetStatementProperties(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    auto statement = (duckdb::PreparedStatement *) stmt;
    uint64_t properties = 0;
    auto statement_properties = statement->GetStatementProperties();

    properties |= (uint64_t) statement->GetStatementType(); // 8 bits
    properties |= (uint64_t) statement_properties.return_type << 8; // 8 bits
    properties |= (uint64_t) ((uint16_t) statement->GetStatementProperties().parameter_count) << 16; // 16 bits
    properties |= (uint64_t) statement_properties.IsReadOnly() << 32; // 1 bit
    properties |= (uint64_t) statement_properties.requires_valid_transaction << 33; // 1 bit
    properties |= (uint64_t) statement_properties.allow_stream_result << 34; // 1 bit
    properties |= (uint64_t) statement_properties.bound_all_parameters << 35; // 1 bit

    return (jlong) properties;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnCount(JNIEnv *, jclass, jlong stmt) {
            D_ASSERT(stmt > 0);
    return (jlong) ((duckdb::PreparedStatement *) stmt)->ColumnCount();
}

JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_preparedGetColumnTypes(JNIEnv *, jclass, jlong stmt, jlong col) {
            D_ASSERT(stmt > 0);
    auto statement = (duckdb::PreparedStatement *) stmt;
    auto &types = statement->GetTypes();
    if (col < 0 || col >= types.size()) {
        return (jlong) pack_duckdb_types(duckdb::PhysicalType::INVALID, duckdb::LogicalTypeId::INVALID);
    }

    auto type = &types[col];
    return (jlong) pack_duckdb_types(type->InternalType(), type->id());
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnName(JNIEnv *, jclass, jlong stmt, jlong col) {
            D_ASSERT(stmt > 0);
    auto statement = (duckdb::PreparedStatement *) stmt;
    auto &names = statement->GetNames();
    if (col < 0 || col >= names.size()) {
        return (jlong) nullptr;
    }

    return (jlong) names[col].c_str();
}

// Result API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetError(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    return res->HasError() ? (jlong) res->GetError().c_str() : (jlong) nullptr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultFetchChunk(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    duckdb::unique_ptr<duckdb::DataChunk> chunk;
    duckdb::PreservedError error;
    if (!((duckdb::QueryResult *) result)->TryFetch(chunk, error)) {
        duckdb_error = error;
        return (jlong) nullptr;
    }
    return (jlong) chunk.release();
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_resultDestroy(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    delete ((duckdb::QueryResult *) result);
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnName(JNIEnv *, jclass, jlong result, jlong col) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (!res || col < 0 || col >= res->ColumnCount()) {
        return (jlong) nullptr;
    }
    return (jlong) res->names[col].c_str();
}

JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_resultColumnTypes(JNIEnv *, jclass, jlong result, jlong col) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (!res || col < 0 || col >= res->ColumnCount()) {
        return (jlong) pack_duckdb_types(duckdb::PhysicalType::INVALID, duckdb::LogicalTypeId::INVALID);
    }
    return (jlong) pack_duckdb_types(res->types[col].InternalType(), res->types[col].id());
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnCount(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    return (jlong) ((duckdb::QueryResult *) result)->ColumnCount();
}

// get the materialized result out of streaming result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetMaterialized(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::STREAM_RESULT) {
        return (jlong) ((duckdb::StreamQueryResult *) res)->Materialize().release();
    }
    return (jlong) nullptr;
}

// return the valid row count of the result only in case of materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultRowCount(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        return (jlong) ((duckdb::MaterializedQueryResult *) res)->RowCount();
    }
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultGetQueryResultType(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    return (jint) ((duckdb::QueryResult *) result)->type;
}

// return the valid chunk only in case of materialized result
JNIEXPORT jlong JNICALL
Java_io_questdb_duckdb_DuckDB_resultGetDataChunk(JNIEnv *, jclass, jlong result, jlong chunk_index) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized = (duckdb::MaterializedQueryResult *) res;
        auto &collection = materialized->Collection();
        if (chunk_index < 0 || chunk_index >= collection.ChunkCount()) {
            return (jlong) nullptr;
        }

        auto chunk = new duckdb::DataChunk();
        chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
        collection.FetchChunk(chunk_index, *chunk);
        return (jlong) chunk;
    }
    return 0;
}

// return the valid chunk count of the result only in case of materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultDataChunkCount(JNIEnv *, jclass, jlong result) {
            D_ASSERT(result > 0);
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        return (jlong) ((duckdb::MaterializedQueryResult *) res)->Collection().ChunkCount();
    }
    return 0;
}

// Data Chunk API

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkDestroy(JNIEnv *, jclass, jlong chunk) {
            D_ASSERT(chunk > 0);
    delete (duckdb::DataChunk *) chunk;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetColumnCount(JNIEnv *, jclass, jlong chunk) {
            D_ASSERT(chunk > 0);
    return (jlong) ((duckdb::DataChunk *) chunk)->ColumnCount();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetVector(JNIEnv *, jclass, jlong chunk, jlong col_idx) {
            D_ASSERT(chunk > 0);
    auto data_chunk = (duckdb::DataChunk *) chunk;
    if (col_idx < 0 || col_idx >= data_chunk->ColumnCount()) {
        return (jlong) nullptr;
    }

    return (jlong) &data_chunk->data[col_idx];
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetSize(JNIEnv *, jclass, jlong chunk) {
            D_ASSERT(chunk > 0);
    return (jlong) ((duckdb::DataChunk *) chunk)->size();
}

// Vector API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetColumnTypes(JNIEnv *, jclass, jlong vector) {
            D_ASSERT(vector > 0);
    auto type = ((duckdb::Vector *) vector)->GetType();
    return (jlong) pack_duckdb_types(type.InternalType(), type.id());
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetData(JNIEnv *, jclass, jlong vector) {
            D_ASSERT(vector > 0);
    return (jlong) duckdb::FlatVector::GetData(*(duckdb::Vector *) vector);
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetValidity(JNIEnv *, jclass, jlong vector) {
            D_ASSERT(vector > 0);
    return (jlong) duckdb::FlatVector::Validity(*(duckdb::Vector *) vector).GetData();
}