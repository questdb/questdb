package io.questdb.griffin.engine.ops;

import io.questdb.cairo.DdlListener;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.TableToken;
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

    private DropAllOperation() {
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            // The return value of compiler execute generally used to decide if a DDL listener callback should be fired.
            // DROP ALL fires the callback for each dropped table inside compiler.execute(), the return value is ignored here.
            compiler.execute(this, sqlExecutionContext);
        }
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.DROP_ALL;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public void onTableOrViewOrMatViewDropped(DdlListener ddlListener, TableToken tableToken) {
        ddlListener.onTableOrViewOrMatViewDropped(tableToken);
    }
}
