#include <memory>
#include <string>
#include "duckdb_jni.h"
#include <duckdb.hpp>

inline void throw_java_exception(JNIEnv * env, const char* type, const char* message) {
    jclass ex_class = env->FindClass(type);
    if (ex_class) {
        env->ThrowNew(ex_class, message);
    }
}

// Database API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpen(JNIEnv * env, jclass cs, jlong path_ptr, jlong path_size) {
    return Java_io_questdb_duckdb_DuckDB_databaseOpenExt(env, cs, path_ptr, path_size, 0);
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseOpenExt(JNIEnv * env, jclass, jlong path_ptr, jlong path_size, jlong config_ptr) {
    std::string path((const char *) path_ptr, path_size);
    try {
        auto config = (duckdb::DBConfig *) config_ptr;
        auto db = new duckdb::DuckDB(path.c_str(), config);
        return (jlong) db;
    } catch (std::exception &ex) {
        throw_java_exception(env, "java/lang/Exception", ex.what());
        return 0;
    } catch (...) {
        throw_java_exception(env, "java/lang/Exception", "Unknown error");
        return 0;
    }
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_databaseClose(JNIEnv*, jclass, jlong db) {
    auto database = (duckdb::DuckDB *) db;
    delete database;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_databaseConnect(JNIEnv * env, jclass, jlong db) {
    auto database = (duckdb::DuckDB *) db;
    if (!database) {
        return 0;
    }

    duckdb::Connection *connection;
    try {
        connection = new duckdb::Connection(*database);
    } catch (std::exception &ex) {
        throw_java_exception(env, "java/lang/Exception", ex.what());
    } catch (...) {
        throw_java_exception(env, "java/lang/Exception", "Unknown error");
    }

    return (jlong) connection;
}

// Connection API

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionInterrupt(JNIEnv*, jclass, jlong connection) {
    auto conn = (duckdb::Connection *) connection;
    if (conn) {
        conn->Interrupt();
    }
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_connectionDisconnect(JNIEnv*, jclass, jlong connection) {
    auto conn = (duckdb::Connection *) connection;
    delete conn;
}

// returns materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionQuery(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size) {
    if (!connection || !query_ptr || query_size <= 0) {
       return 0;
    }

    std::string query((const char *) query_ptr, query_size);
    auto conn = (duckdb::Connection *) connection;
    auto res = conn->Query(query);
    return (jlong) res.release();
}

// Configuration API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_configCreate(JNIEnv *env, jclass) {
    duckdb::DBConfig *config = nullptr;
    try {
        config = new duckdb::DBConfig();
    } catch (std::exception &ex) {
        throw_java_exception(env, "java/lang/Exception", ex.what());
    } catch (...) {
        throw_java_exception(env, "java/lang/Exception", "Unknown error");
    }

    return (jlong) config;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_duckdb_DuckDB_configSet(JNIEnv * env, jclass, jlong config, jlong name_ptr, jlong name_size, jlong option_ptr, jlong option_size) {
    auto cfg = (duckdb::DBConfig *) config;
    if (!cfg || !name_ptr || !option_ptr || name_size <= 0 || option_size <= 0 ) {
        return false;
    }

    std::string name((const char *) name_ptr, name_size);
    std::string option((const char *) option_ptr, option_size);

    auto cfg_option = duckdb::DBConfig::GetOptionByName(name);
    if (!cfg_option) {
        return false;
    }

    try {
        cfg->SetOption(*cfg_option, duckdb::Value(option));
    } catch (std::exception &ex) {
        throw_java_exception(env, "java/lang/Exception", ex.what());
    } catch (...) {
        throw_java_exception(env, "java/lang/Exception", "Unknown error");
    }

    return true;
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_configDestroy(JNIEnv*, jclass, jlong config) {
    auto cfg = (duckdb::DBConfig *) config;
    delete cfg;
}

// Prepared Statement API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_connectionPrepare(JNIEnv*, jclass, jlong connection, jlong query_ptr, jlong query_size) {
	if (!connection || !query_ptr || query_size <= 0) {
		return 0;
	}

    std::string query((const char *) query_ptr, query_size);
    auto conn = (duckdb::Connection *) connection;
	auto statement = conn->Prepare(query);
	return (jlong) statement.release();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedExecute(JNIEnv*, jclass, jlong stmt) {
    auto statement = (duckdb::PreparedStatement *) stmt;
    if (!statement || statement->HasError()) {
        return 0;
    }

    auto res = statement->Execute();
    return (jlong) res.release();
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_preparedDestroy(JNIEnv*, jclass, jlong stmt) {
    auto statement = (duckdb::PreparedStatement *) stmt;
    delete statement;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetError(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return 0;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jlong) statement->GetError().c_str();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetQueryText(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return 0;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jlong) statement->query.c_str();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetStatementType(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return (jint) duckdb::StatementType::INVALID_STATEMENT;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jint) statement->GetStatementType();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetStatementReturnType(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return (jint) duckdb::StatementReturnType::NOTHING;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jint) statement->GetStatementProperties().return_type;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_duckdb_DuckDB_preparedAllowStreamResult(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return false;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return statement->GetStatementProperties().allow_stream_result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedParameterCount(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return 0;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jlong) statement->GetStatementProperties().parameter_count;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnCount(JNIEnv*, jclass, jlong stmt) {
    if (!stmt) {
        return 0;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    return (jlong) statement->ColumnCount();
}

inline const duckdb::LogicalType* get_type(duckdb::PreparedStatement* stmt, jlong col) {
    if (!stmt) {
        return nullptr;
    }

    auto &types = stmt->GetTypes();
    if (col < 0 || col >= types.size()) {
        return nullptr;
    }

    return &types[col];
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnLogicalType(JNIEnv*, jclass, jlong stmt, jlong col) {
    auto type = get_type((duckdb::PreparedStatement *) stmt, col);
    if (!type) {
        return (jint) duckdb::LogicalTypeId::INVALID;
    }

    return (jint) type->id();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnPhysicalType(JNIEnv*, jclass, jlong stmt, jlong col) {
    auto type = get_type((duckdb::PreparedStatement *)stmt, col);
    if (!type) {
        return (jint) duckdb::PhysicalType::INVALID;
    }

    return (jint) type->InternalType();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_preparedGetColumnName(JNIEnv*, jclass, jlong stmt, jlong col) {
    if (!stmt) {
        return 0;
    }

    auto statement = (duckdb::PreparedStatement *) stmt;
    auto &names = statement->GetNames();
    if (col < 0 || col >= names.size()) {
        return 0;
    }

    return (jlong) names[col].c_str();
}

// Result API

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetError(JNIEnv*, jclass, jlong result) {
    auto res = (duckdb::QueryResult*)result;
    if (res && res->HasError()) {
        return (jlong) res->GetError().c_str();
    }

    return 0;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultFetchChunk(JNIEnv*, jclass, jlong result) {
    auto res = (duckdb::QueryResult*)result;
    if (!res || res->HasError()) {
        return 0;
    }
    duckdb::unique_ptr<duckdb::DataChunk> chunk = nullptr;
    duckdb::PreservedError error;
    if (!res->TryFetch(chunk, error)) {
        return 0; // TODO: throw java exception
    }
    return (jlong) chunk.release();
}

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_resultDestroy(JNIEnv*, jclass, jlong result) {
    if (result) {
        auto res = (duckdb::QueryResult *) result;
        delete res;
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnName(JNIEnv*, jclass, jlong result, jlong col) {
    auto res = (duckdb::QueryResult *) result;
    if (!res || col < 0 || col >= res->ColumnCount() ) {
        return 0;
    }

    return (jlong) res->names[col].c_str();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnPhysicalType(JNIEnv*, jclass, jlong result, jlong col) {
    auto res = (duckdb::QueryResult *) result;
    if (!res || col < 0 || col >= res->ColumnCount() ) {
        return (jint) duckdb::PhysicalType::INVALID;
    }

    return (jint) res->types[col].InternalType();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnLogicalType(JNIEnv*, jclass, jlong result, jlong col) {
    auto res = (duckdb::QueryResult *) result;
    if (!res || col < 0 || col >= res->ColumnCount() ) {
        return (jint) duckdb::LogicalTypeId::INVALID;
    }

    return (jint) res->types[col].id();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultColumnCount(JNIEnv*, jclass, jlong result) {
    if (!result) {
        return 0;
    }

    auto res = (duckdb::QueryResult *) result;
    return (jlong) res->ColumnCount();
}

// get the materialized result out of streaming result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetMaterialized(JNIEnv*, jclass, jlong result) {
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::STREAM_RESULT) {
        auto stream = (duckdb::StreamQueryResult *) res;
        auto materialized = stream->Materialize();
        return (jlong) materialized.release();
    }

    return 0;
}

// return the valid row count of the result only in case of materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultRowCount(JNIEnv*, jclass, jlong result) {
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        return (jlong) ((duckdb::MaterializedQueryResult *) res)->RowCount();
    }

    return 0;
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultError(JNIEnv*, jclass, jlong result) {
    if (!result) {
        return 0;
    }

    auto res = (duckdb::QueryResult *) result;
    return (jlong) (!res->HasError() ? nullptr : res->GetError().c_str());
}

// return -1 in case of error
JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_resultGetQueryResultType(JNIEnv*, jclass, jlong result) {
    if (!result) {
        return -1;
    }

    auto res = (duckdb::QueryResult*)result;
    return (jint) res->type;
}

// return the valid chunk only in case of materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultGetDataChunk(JNIEnv*, jclass, jlong result, jlong chunk_index) {
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized = (duckdb::MaterializedQueryResult *) res;
        auto &collection = materialized->Collection();
        if (chunk_index < 0 || chunk_index >= collection.ChunkCount()) {
            return 0;
        }

        auto chunk = new duckdb::DataChunk();
        chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
        collection.FetchChunk(chunk_index, *chunk);
        return (jlong) chunk;
    }
    return 0;
}

// return the valid chunk count of the result only in case of materialized result
JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_resultDataChunkCount(JNIEnv*, jclass, jlong result) {
    auto res = (duckdb::QueryResult *) result;
    if (res && res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        return (jlong) ((duckdb::MaterializedQueryResult *) res)->Collection().ChunkCount();
    }

    return 0;
}

// Data Chunk API

JNIEXPORT void JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkDestroy(JNIEnv*, jclass, jlong chunk) {
    if (chunk) {
        auto data_chunk = (duckdb::DataChunk *) chunk;
        delete data_chunk;
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetColumnCount(JNIEnv*, jclass, jlong chunk) {
    if (!chunk) {
        return 0;
    }

    auto data_chunk = (duckdb::DataChunk *) chunk;
    return (jlong) data_chunk->ColumnCount();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetVector(JNIEnv*, jclass, jlong chunk, jlong col_idx) {
    auto data_chunk = (duckdb::DataChunk *) chunk;
    if (!data_chunk || col_idx < 0 || col_idx >= data_chunk->ColumnCount() ) {
        return 0;
    }

    return (jlong) &data_chunk->data[col_idx];
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_dataChunkGetSize(JNIEnv*, jclass, jlong chunk) {
    if (!chunk) {
        return 0;
    }

    auto data_chunk = (duckdb::DataChunk *) chunk;
    return (jlong) data_chunk->size();
}

// Vector API

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetColumnLogicalType(JNIEnv*, jclass, jlong vector) {
    if (!vector) {
        return (jint) duckdb::LogicalTypeId::INVALID;
    }

    auto vec = (duckdb::Vector *) vector;
    return (jint) vec->GetType().id();
}

JNIEXPORT jint JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetColumnPhysicalType(JNIEnv*, jclass, jlong vector) {
    if (!vector) {
        return (jint) duckdb::PhysicalType::INVALID;
    }

    auto vec = (duckdb::Vector *) vector;
    return (jint) vec->GetType().InternalType();
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetData(JNIEnv*, jclass, jlong vector) {
    if (!vector) {
        return 0;
    }

    auto vec = (duckdb::Vector *) vector;
    return (jlong) duckdb::FlatVector::GetData(*vec);
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_vectorGetValidity(JNIEnv*, jclass, jlong vector) {
    if (!vector) {
        return 0;
    }

    auto vec = (duckdb::Vector *) vector;
    return (jlong) duckdb::FlatVector::Validity(*vec).GetData();
}