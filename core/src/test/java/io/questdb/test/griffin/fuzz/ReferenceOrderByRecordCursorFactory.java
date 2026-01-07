package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public final class ReferenceOrderByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final IntList orderByColumns;

    /**
     * Reference ORDER BY that materializes and sorts rows by the provided columns.
     */
    public ReferenceOrderByRecordCursorFactory(RecordCursorFactory base, IntList orderByColumns) {
        super(base.getMetadata());
        this.base = base;
        this.orderByColumns = orderByColumns;
    }

    /**
     * Exposes the wrapped factory for plan/debug convenience.
     */
    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    /**
     * Returns a cursor over the sorted, materialized row set.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordMetadata metadata = getMetadata();
        ReferenceRowSet rowSet = ReferenceRowSet.materialize(base, metadata, executionContext, null);
        ReferenceRowSet sorted = rowSet.sorted(orderByColumns);
        return new ReferenceRowSet.Cursor(sorted);
    }

    /**
     * Reference output is materialized and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a plan marker for reference ORDER BY.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Order By");
        sink.child(base);
    }

    @Override
    protected void _close() {
        base.close();
    }
}
