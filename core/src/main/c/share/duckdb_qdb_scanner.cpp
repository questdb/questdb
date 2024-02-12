#include <memory>
#include <string>
#include <duckdb.hpp>
#include "duckdb_qdb_scanner.h"

namespace duckdb {

    struct QuestDBGlobalState : public GlobalTableFunctionState {
        QuestDBGlobalState(idx_t max_threads) : max_threads(max_threads) {
        }

        mutex lock;
        idx_t position = 0;
        idx_t max_threads;

        idx_t MaxThreads() const override {
            return max_threads;
        }
    };

    struct QuestDBLocalState : public LocalTableFunctionState {
        bool done = false;
        vector<column_t> column_ids;
        ~QuestDBLocalState() {
        }
    };

    struct QuestDBBindData : public TableFunctionData {
        string table_name;
        vector<std::string> names;
        vector<duckdb::LogicalType> types;

        idx_t rows = 0;
        idx_t rows_per_group = 122880;
    };

    static unique_ptr<NodeStatistics> QuestDBCardinality(ClientContext &context, const FunctionData *bind_data_p) {
        D_ASSERT(bind_data_p);
        auto &bind_data = bind_data_p->Cast<QuestDBBindData>();
        return make_uniq<NodeStatistics>(bind_data.rows);
    }

    static idx_t QuestDBMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
        D_ASSERT(bind_data_p);
        auto &bind_data = bind_data_p->Cast<QuestDBBindData>();
        return bind_data.rows / bind_data.rows_per_group;
    }

    static string QuestDBToString(const FunctionData *bind_data_p) {
        D_ASSERT(bind_data_p);
        auto &bind_data = bind_data_p->Cast<QuestDBBindData>();
        return StringUtil::Format("%s", bind_data.table_name);
    }

    static unique_ptr<GlobalTableFunctionState> QuestDBInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
        auto result = make_uniq<QuestDBGlobalState>(QuestDBMaxThreads(context, input.bind_data.get()));
        result->position = 0;
        return std::move(result);
    }

    static void QuestDBInitInternal(ClientContext &context, const QuestDBBindData &bind_data, QuestDBLocalState &local_state,
                                   idx_t min, idx_t max) {

        local_state.done = false;
        // reopen TableReader if necessary
        // local_state.reader = ????
        if (bind_data.rows_per_group != idx_t(-1)) {
            // we are scanning a subset of the rows
        } else {
            // we are scanning the entire table
            D_ASSERT(min == 0);
        }
        // local_state = ????
    }

    static bool QuestDBParallelStateNext(ClientContext &context, const QuestDBBindData &bind_data,
                                         QuestDBLocalState &lstate, QuestDBGlobalState &gstate) {
        lock_guard<mutex> parallel_lock(gstate.lock);
        if (gstate.position < bind_data.rows) {
            auto start = gstate.position;
            auto end = start + bind_data.rows_per_group - 1;
            QuestDBInitInternal(context, bind_data, lstate, start, end);
            gstate.position = end + 1;
            return true;
        }
        return false;
    }

    static unique_ptr<LocalTableFunctionState>
    QuestDBInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
        auto &bind_data = input.bind_data->Cast<QuestDBBindData>();
        auto &gstate = global_state->Cast<QuestDBGlobalState>();
        auto result = make_uniq<QuestDBLocalState>();
        result->column_ids = input.column_ids;

        if (!QuestDBParallelStateNext(context.client, bind_data, *result, gstate)) {
            result->done = true;
        }
        return std::move(result);
    }

    static unique_ptr<FunctionData> QuestDBBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &types, vector<string> &names) {

        auto result = make_uniq<QuestDBBindData>();
        result->table_name = input.inputs[0].GetValue<string>();

        // TODO: get column names and types from QuestDB table
        names.push_back("my_column");
        types.push_back(LogicalType::INTEGER);

        // TODO: get row count from QuestDB table
         result->rows = 1000;
         result->rows_per_group = 100;

        result->names = names;
        result->types = types;

        return std::move(result);
    }

    static void QuestDBScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
        auto &state = data.local_state->Cast<QuestDBLocalState>();
        auto &gstate = data.global_state->Cast<QuestDBGlobalState>();
        auto &bind_data = data.bind_data->Cast<QuestDBBindData>();

        while (output.size() == 0) {
            if (state.done) {
                if (!QuestDBParallelStateNext(context, bind_data, state, gstate)) {
                    return;
                }
            }

            idx_t out_idx = 0;
            while (true) {
                if (out_idx == STANDARD_VECTOR_SIZE) {
                    output.SetCardinality(out_idx);
                    return;
                }
                // read from QuestDB table
                auto has_more = out_idx < bind_data.rows;
                if (!has_more) {
                    state.done = true;
                    output.SetCardinality(out_idx);
                    break;
                }
                // TODO: real algo must be column based
                for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                    auto &out_vec = output.data[col_idx];
                    if (out_vec.GetType().id() == LogicalTypeId::INTEGER) {
                        FlatVector::GetData<int32_t>(out_vec)[out_idx] = (int32_t)out_idx;
                    }
//                    auto qdb_column_type = stmt.GetType(col_idx);
//                    if (qdb_column_type == QDB_NULL) {
//                        auto &mask = FlatVector::Validity(out_vec);
//                        mask.Set(out_idx, false);
//                        continue;
//                    }
//
//                    // get the value from the QuestDB table for the col_idx
//                    auto val = 22;
//                    switch (out_vec.GetType().id()) {
//                        case LogicalTypeId::BIGINT:
//                            FlatVector::GetData<int64_t>(out_vec)[out_idx] = convert(val);
//                            break;
//                        case LogicalTypeId::DOUBLE:
//                            FlatVector::GetData<double>(out_vec)[out_idx] = convert(val);
//                            break;
//                        case LogicalTypeId::VARCHAR:
//                            FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddString(
//                                    out_vec, (const char *)convert(val), convert(val));
//                            break;
//                            /// ... other types
//                        default:
//                            throw std::runtime_error(out_vec.GetType().ToString());
//                    }
                }
                out_idx++;
            }
        }
    }
    class QuestDBScanFunction : public TableFunction {
    public:
        QuestDBScanFunction() : TableFunction("questdb_scan", /*args*/ { duckdb::LogicalType::VARCHAR }, QuestDBScan, QuestDBBind,
                                              QuestDBInitGlobalState, QuestDBInitLocalState) {
            cardinality = QuestDBCardinality;
            to_string = QuestDBToString;
            // ...
            //projection_pushdown = true;
        }
    };
}

static void RegisterFunction(duckdb::DatabaseInstance &db, duckdb::TableFunction function) {
    D_ASSERT(!function.name.empty());
    duckdb::TableFunctionSet set(function.name);
    set.AddFunction(std::move(function));

    duckdb::CreateTableFunctionInfo info(std::move(set));
    auto &system_catalog = duckdb::Catalog::GetSystemCatalog(db);
    auto data = duckdb::CatalogTransaction::GetSystemTransaction(db);
    system_catalog.CreateFunction(data, info);
}

JNIEXPORT jlong JNICALL Java_io_questdb_duckdb_DuckDB_registerQuestDBScanFunction(JNIEnv *, jclass, jlong db_ptr) {
    if (!db_ptr) {
        return 0;
    }

    duckdb::DuckDB& db = *(duckdb::DuckDB *) db_ptr;
    duckdb::QuestDBScanFunction questdb_scan_fun;
    RegisterFunction(*db.instance, questdb_scan_fun);
    return 1;
}
