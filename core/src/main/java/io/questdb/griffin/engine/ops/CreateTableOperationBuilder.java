package io.questdb.griffin.engine.ops;

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;

public interface CreateTableOperationBuilder extends ExecutionModel {
    CreateTableOperation build(
            SqlCompiler sqlCompiler,
            SqlExecutionContext executionContext,
            CharSequence sqlText
    ) throws SqlException;

    @Override
    default int getModelType() {
        return CREATE_TABLE;
    }
}
