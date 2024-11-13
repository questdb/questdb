package io.questdb.griffin.engine.ops;

import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class DropTableOperation implements Operation {
    public static final String DROP_FLAG_IF_EXISTS = "if_exists";
    public static final CharSequence IF_EXISTS_VALUE_STUB = "";
    private final DoneOperationFuture future = new DoneOperationFuture();
    private final boolean ifExists;
    private final String sqlText;
    private final String tableName;
    private final int tableNamePosition;

    public DropTableOperation(
            String sqlText,
            String tableName,
            int tableNamePosition,
            boolean ifExists
    ) {
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
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

    @Override
    public int getOperationCode() {
        return OperationCodes.DROP_TABLE;
    }

    public String getSqlText() {
        return sqlText;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableNamePosition() {
        return tableNamePosition;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
