package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;

public final class FuzzCase {
    public final RecordCursorFactory underTest;
    public final RecordCursorFactory oracle;
    public final boolean ordered;
    public final String description;

    /**
     * Carries the under-test/oracle factories and metadata needed for comparison output.
     */
    public FuzzCase(RecordCursorFactory underTest, RecordCursorFactory oracle, boolean ordered, String description) {
        this.underTest = underTest;
        this.oracle = oracle;
        this.ordered = ordered;
        this.description = description;
    }
}
