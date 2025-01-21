package io.questdb.griffin.engine.ops;

import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class DropMatViewOperation implements Operation {
    private final DoneOperationFuture future = new DoneOperationFuture();
    private final boolean ifExists;
    private final String matViewName;
    private final int matViewNamePosition;
    private final String sqlText;

    public DropMatViewOperation(
            String sqlText,
            String matViewName,
            int matViewNamePosition,
            boolean ifExists
    ) {
        this.matViewName = matViewName;
        this.matViewNamePosition = matViewNamePosition;
        this.ifExists = ifExists;
        this.sqlText = sqlText;
    }

    @Override
    public void close() {
        // nothing to fee
    }

    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return future;
    }

    public String getMatViewName() {
        return matViewName;
    }

    public int getMatViewNamePosition() {
        return matViewNamePosition;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.DROP_MAT_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return future;
    }

    public String getSqlText() {
        return sqlText;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
