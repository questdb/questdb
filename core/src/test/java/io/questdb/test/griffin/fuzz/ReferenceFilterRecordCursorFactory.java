package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public final class ReferenceFilterRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final Function filter;

    /**
     * Reference FILTER that materializes rows and applies the predicate per record.
     */
    public ReferenceFilterRecordCursorFactory(RecordCursorFactory base, Function filter) {
        super(base.getMetadata());
        this.base = base;
        this.filter = filter;
    }

    /**
     * Exposes the wrapped factory for plan/debug convenience.
     */
    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    /**
     * Returns a cursor over the materialized rows that satisfy the predicate.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordMetadata metadata = getMetadata();
        ReferenceRowSet rowSet = ReferenceRowSet.materialize(
                base,
                metadata,
                executionContext,
                record -> filter.getBool(record)
        );
        return new ReferenceRowSet.Cursor(rowSet);
    }

    /**
     * Reference output is materialized and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a plan marker for reference FILTER.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Filter");
        sink.child(base);
    }

    @Override
    protected void _close() {
        base.close();
        filter.close();
    }
}
