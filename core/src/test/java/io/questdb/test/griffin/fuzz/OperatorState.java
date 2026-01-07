package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;

public final class OperatorState {
    public RecordCursorFactory underTest;
    public RecordCursorFactory oracle;
    public boolean ordered;

    /**
     * Tracks the current pipeline factories and whether the output is ordered.
     */
    public OperatorState(RecordCursorFactory underTest, RecordCursorFactory oracle, boolean ordered) {
        this.underTest = underTest;
        this.oracle = oracle;
        this.ordered = ordered;
    }
}
