package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public final class SizeCheckingRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final SizeCheckingRecordCursor cursor = new SizeCheckingRecordCursor();

    public SizeCheckingRecordCursorFactory(RecordCursorFactory base) {
        super(base.getMetadata());
        this.base = base;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(baseCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Size Check");
        sink.child(base);
    }

    @Override
    protected void _close() {
        base.close();
    }

    private static final class SizeCheckingRecordCursor implements DelegatingRecordCursor {
        private RecordCursor base;
        private long expectedSize;
        private long seen;
        private boolean checked;

        @Override
        public void of(RecordCursor base, SqlExecutionContext executionContext) {
            this.base = base;
            this.seen = 0;
            this.checked = false;
            try {
                this.expectedSize = base.size();
            } catch (DataUnavailableException e) {
                this.expectedSize = -1;
            }
        }

        @Override
        public boolean hasNext() {
            if (base.hasNext()) {
                seen++;
                return true;
            }
            if (!checked) {
                checked = true;
                if (expectedSize >= 0 && expectedSize != seen) {
                    throw new AssertionError("cursor size mismatch [expected=" + expectedSize + ", actual=" + seen + ']');
                }
            }
            return false;
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            base.calculateSize(circuitBreaker, counter);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() throws DataUnavailableException {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
            long before = rowCount.get();
            base.skipRows(rowCount);
            long skipped = before - rowCount.get();
            if (skipped > 0) {
                seen += skipped;
            }
        }

        @Override
        public void toTop() {
            base.toTop();
            seen = 0;
            checked = false;
        }

        @Override
        public void close() {
            base = Misc.free(base);
        }

        @Override
        public long preComputedStateSize() {
            return base.preComputedStateSize();
        }
    }
}
