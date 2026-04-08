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
    private final char lagUnit;
    private final long lagValue;
    private final char retentionUnit;
    private final long retentionValue;
    private final String selectSql;
    private final String viewName;
    private final int viewNamePosition;

    public CreateLiveViewOperation(
            String viewName,
            int viewNamePosition,
            String baseTableName,
            String selectSql,
            long lagValue,
            char lagUnit,
            long retentionValue,
            char retentionUnit,
            boolean ignoreIfExists
    ) {
        this.viewName = viewName;
        this.viewNamePosition = viewNamePosition;
        this.baseTableName = baseTableName;
        this.selectSql = selectSql;
        this.lagValue = lagValue;
        this.lagUnit = lagUnit;
        this.retentionValue = retentionValue;
        this.retentionUnit = retentionUnit;
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

    public char getLagUnit() {
        return lagUnit;
    }

    public long getLagValue() {
        return lagValue;
    }

    @Override
    public int getOperationCode() {
        return io.questdb.cairo.OperationCodes.CREATE_LIVE_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public char getRetentionUnit() {
        return retentionUnit;
    }

    public long getRetentionValue() {
        return retentionValue;
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
