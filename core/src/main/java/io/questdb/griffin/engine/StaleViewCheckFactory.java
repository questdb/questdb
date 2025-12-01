package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class StaleViewCheckFactory implements RecordCursorFactory {
    private final TableToken[] viewTokens;
    private final RecordCursorFactory base;
    private final CairoEngine engine;

    public StaleViewCheckFactory(RecordCursorFactory base, ObjList<ViewDefinition> views, CairoEngine engine) {
        this.base = base;
        this.viewTokens = new TableToken[views.size()];
        for (int i = 0; i < views.size(); i++) {
            this.viewTokens[i] = views.getQuick(i).getViewToken();
        }
        this.engine = engine;
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        return base.convertToSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public PageFrameSequence<?> execute(@Transient SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    public RecordCursorFactory getBaseFactory() {
        return base.getBaseFactory();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = viewTokens.length; i < n; i++) {
            var token = viewTokens[i];
            engine.verifyTableToken(token);
        }
        return base.getCursor(executionContext);
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return this.base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        base.revertFromSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        base.toPlan(sink);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }
}
