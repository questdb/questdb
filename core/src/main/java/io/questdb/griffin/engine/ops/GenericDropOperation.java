package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.OperationCodes.*;

public class GenericDropOperation implements Operation {
    private final String entityName;
    private final int entityNamePosition;
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
        final CairoEngine engine = sqlExecutionContext.getCairoEngine();
        final TableToken tableToken = engine.getTableTokenIfExists(entityName);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            if (compiler.execute(this, sqlExecutionContext)) {
                switch (operationCode) {
                    case DROP_TABLE, DROP_MAT_VIEW, DROP_VIEW -> {
                        if (tableToken != null) {
                            engine.getDdlListener(entityName).onTableOrViewOrMatViewDropped(tableToken);
                        }
                    }
                    default -> {
                    }
                }
            }
        }
        return ImmutableDoneOperationFuture.INSTANCE;
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
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public String getSqlText() {
        return sqlText;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
