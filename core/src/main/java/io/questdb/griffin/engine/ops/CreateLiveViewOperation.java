package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class CreateLiveViewOperation implements Operation {
    private final String baseTableName;
    private final boolean ignoreIfExists;
    private final long lagMicros;
    private final long retentionMicros;
    private final String selectSql;
    private final String viewName;
    private final int viewNamePosition;

    public CreateLiveViewOperation(
            String viewName,
            int viewNamePosition,
            String baseTableName,
            String selectSql,
            long lagMicros,
            long retentionMicros,
            boolean ignoreIfExists
    ) {
        this.viewName = viewName;
        this.viewNamePosition = viewNamePosition;
        this.baseTableName = baseTableName;
        this.selectSql = selectSql;
        this.lagMicros = lagMicros;
        this.retentionMicros = retentionMicros;
        this.ignoreIfExists = ignoreIfExists;
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        final CairoEngine engine = sqlExecutionContext.getCairoEngine();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public long getLagMicros() {
        return lagMicros;
    }

    @Override
    public int getOperationCode() {
        return io.questdb.cairo.OperationCodes.CREATE_LIVE_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public long getRetentionMicros() {
        return retentionMicros;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public String getViewName() {
        return viewName;
    }

    public int getViewNamePosition() {
        return viewNamePosition;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }
}
