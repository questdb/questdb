package io.questdb.griffin.engine.lv;

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;

/**
 * Cursor that iterates over the rows of a live view's InMemoryTable.
 * Acquires a read lock on the view instance for the duration of the query.
 */
public class LiveViewRecordCursor implements RecordCursor {
    private final LiveViewRecord record;
    private final LiveViewInstance viewInstance;
    private long currentRow;
    private boolean isOpen;
    private long rowCount;

    public LiveViewRecordCursor(LiveViewInstance viewInstance) {
        this.viewInstance = viewInstance;
        this.record = new LiveViewRecord(viewInstance.getTable());
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        counter.add(rowCount - currentRow);
        currentRow = rowCount;
    }

    @Override
    public void close() {
        if (isOpen) {
            viewInstance.unlockAfterRead();
            isOpen = false;
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException("live view does not support record B");
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((LiveViewRecord) record).setRow(atRowId);
    }

    @Override
    public boolean hasNext() {
        if (currentRow < rowCount) {
            record.setRow(currentRow++);
            return true;
        }
        return false;
    }

    @Override
    public long preComputedStateSize() {
        return rowCount;
    }

    public void open() {
        viewInstance.lockForRead();
        isOpen = true;
        rowCount = viewInstance.getTable().getRowCount();
        currentRow = 0;
    }

    @Override
    public long size() {
        return rowCount;
    }

    @Override
    public void toTop() {
        currentRow = 0;
    }
}
