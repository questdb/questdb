package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;

public final class FuzzFactories {
    public final RecordCursorFactory underTest;
    public final RecordCursorFactory oracle;

    /**
     * Bundles the under-test and oracle factories produced by a single operator.
     */
    public FuzzFactories(RecordCursorFactory underTest, RecordCursorFactory oracle) {
        this.underTest = underTest;
        this.oracle = oracle;
    }
}
