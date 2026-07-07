package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
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

    /**
     * Reports whether this operation drops a plain table whose name carries the reserved
     * parquet-export prefix. The engine reserves that prefix exclusively for the temporary tables
     * the HTTP parquet exporter materializes, and a regular CREATE TABLE rejects any name starting
     * with it, so a matching DROP can only target an exporter temp table, never a user table. A
     * read-only replica leaves such a temp table behind when its own cleanup fails, and the admin
     * then drops it as a purely local operation; the replica read-only ingress gate consults this to
     * let that local cleanup proceed while still refusing every genuine client DROP.
     */
    public boolean isExportTempTableDrop(CharSequence parquetExportTableNamePrefix) {
        return operationCode == DROP_TABLE
                && parquetExportTableNamePrefix != null
                && Chars.startsWith(entityName, parquetExportTableNamePrefix);
    }
}
