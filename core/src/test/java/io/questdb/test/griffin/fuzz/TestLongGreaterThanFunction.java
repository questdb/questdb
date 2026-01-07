package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BooleanFunction;

public final class TestLongGreaterThanFunction extends BooleanFunction {
    private final int columnIndex;
    private final long threshold;

    public TestLongGreaterThanFunction(int columnIndex, long threshold) {
        this.columnIndex = columnIndex;
        this.threshold = threshold;
    }

    public long getThreshold() {
        return threshold;
    }

    @Override
    public boolean getBool(Record rec) {
        return rec.getLong(columnIndex) > threshold;
    }

    @Override
    public String getName() {
        return "test_gt";
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }
}
