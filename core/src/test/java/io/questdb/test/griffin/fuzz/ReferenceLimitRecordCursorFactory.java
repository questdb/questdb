package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Numbers;

public final class ReferenceLimitRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitSpec limitSpec;

    /**
     * Reference LIMIT that materializes and slices rows using limit semantics.
     */
    public ReferenceLimitRecordCursorFactory(RecordCursorFactory base, LimitSpec limitSpec) {
        super(base.getMetadata());
        this.base = base;
        this.limitSpec = limitSpec;
    }

    /**
     * Exposes the wrapped factory for plan/debug convenience.
     */
    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    /**
     * Returns a cursor over the sliced, materialized row set.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordMetadata metadata = getMetadata();
        ReferenceRowSet rowSet = ReferenceRowSet.materialize(base, metadata, executionContext, null);
        ReferenceRowSet sliced = slice(rowSet, limitSpec);
        return new ReferenceRowSet.Cursor(sliced);
    }

    /**
     * Reference output is materialized and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a plan marker for reference LIMIT.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Limit");
        sink.child(base);
    }

    @Override
    protected void _close() {
        base.close();
    }

    private static ReferenceRowSet slice(ReferenceRowSet rowSet, LimitSpec spec) {
        int size = rowSet.size();
        long lo = spec.lo;
        long hi = spec.hi;
        if (!spec.hasHi) {
            if (lo >= 0) {
                return sliceRange(rowSet, 0, lo);
            }
            return sliceRange(rowSet, size + lo, size);
        }
        if (lo < 0 && hi > 0) {
            throw new IllegalArgumentException("LIMIT <negative>, <positive> is not allowed");
        }
        if (lo > hi && Numbers.sameSign(lo, hi)) {
            long tmp = lo;
            lo = hi;
            hi = tmp;
        }
        if (lo == hi) {
            return rowSet.slice(0, 0);
        }
        if (lo < 0) {
            long start = size + lo;
            long end = hi <= 0 ? size + hi : hi;
            return sliceRange(rowSet, start, end);
        }
        if (hi < 0) {
            return sliceRange(rowSet, lo, size + hi);
        }
        return sliceRange(rowSet, lo, hi);
    }

    private static ReferenceRowSet sliceRange(ReferenceRowSet rowSet, long start, long end) {
        int size = rowSet.size();
        long clampedStart = Math.max(0, start);
        long clampedEnd = Math.min(size, end);
        if (clampedEnd < 0) {
            clampedEnd = 0;
        }
        return rowSet.slice(clampedStart, clampedEnd);
    }
}
