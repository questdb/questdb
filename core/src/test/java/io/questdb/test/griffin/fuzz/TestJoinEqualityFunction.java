package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BooleanFunction;

public final class TestJoinEqualityFunction extends BooleanFunction {
    private final int leftColumn;
    private final int rightColumn;

    public TestJoinEqualityFunction(int leftColumn, int rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    @Override
    public boolean getBool(Record rec) {
        return rec.getLong(leftColumn) == rec.getLong(rightColumn);
    }

    @Override
    public String getName() {
        return "test_join_eq";
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }
}
