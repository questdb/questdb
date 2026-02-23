package io.questdb.griffin.engine.ops;

import io.questdb.cairo.DdlListener;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

/**
 * Drops all tables, materialized views and views.
 */
public class DropAllOperation implements Operation {
    public static final DropAllOperation INSTANCE = new DropAllOperation();
    private final DoneOperationFuture future = new DoneOperationFuture();

    protected DropAllOperation() {
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return future;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.DROP_ALL;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return future;
    }

    public void onTableOrViewOrMatViewDropped(DdlListener ddlListener, String tableName) {
        ddlListener.onTableOrViewOrMatViewDropped(tableName, false);
    }
}
