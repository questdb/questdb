package io.questdb.griffin.engine.lv;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * Factory that produces cursors reading from a live view's InMemoryTable.
 */
public class LiveViewRecordCursorFactory extends AbstractRecordCursorFactory {
    private final LiveViewRecordCursor cursor;
    private final LiveViewInstance viewInstance;

    public LiveViewRecordCursorFactory(LiveViewInstance viewInstance) {
        super(viewInstance.getTable().getMetadata());
        this.viewInstance = viewInstance;
        this.cursor = new LiveViewRecordCursor(viewInstance);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.open();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LiveView");
        sink.attr("name").val(viewInstance.getDefinition().getViewName());
    }

    @Override
    protected void _close() {
        // LiveViewInstance is owned by the registry, not this factory
    }
}
