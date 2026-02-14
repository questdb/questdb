package io.questdb.griffin.engine.ops;

import io.questdb.cairo.DdlListener;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GenericDropOperation implements Operation {
    private final String entityName;
    private final int entityNamePosition;
    private final DoneOperationFuture future = new DoneOperationFuture();
    private final boolean ifExists;
    private final int operationCode;
    private final String sqlText;

    public GenericDropOperation(
            int operationCode,
            @Nullable String sqlText,
            @NotNull String entityName,
            int entityNamePosition,
            boolean ifExists
    ) {
        this.operationCode = operationCode;
        this.sqlText = sqlText;
        this.entityName = entityName;
        this.entityNamePosition = entityNamePosition;
        this.ifExists = ifExists;
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        onTableDropped(sqlExecutionContext.getCairoEngine().getDdlListener(entityName), entityName);
        return future;
    }

    public String getEntityName() {
        return entityName;
    }

    public int getEntityNamePosition() {
        return entityNamePosition;
    }

    @Override
    public int getOperationCode() {
        return operationCode;
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

    protected void onTableDropped(DdlListener ddlListener, String tableName) {
        ddlListener.onTableDropped(tableName, false);
    }
}
