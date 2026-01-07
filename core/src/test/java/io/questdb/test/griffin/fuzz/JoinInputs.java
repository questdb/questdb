package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;

public final class JoinInputs {
    public final RecordCursorFactory leftUnderTest;
    public final RecordCursorFactory rightUnderTest;
    public final RecordCursorFactory leftOracle;
    public final RecordCursorFactory rightOracle;

    /**
     * Holds both engine and oracle factories for left/right inputs to a join.
     */
    public JoinInputs(
            RecordCursorFactory leftUnderTest,
            RecordCursorFactory rightUnderTest,
            RecordCursorFactory leftOracle,
            RecordCursorFactory rightOracle
    ) {
        this.leftUnderTest = leftUnderTest;
        this.rightUnderTest = rightUnderTest;
        this.leftOracle = leftOracle;
        this.rightOracle = rightOracle;
    }
}
