package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.std.Rnd;

public final class FilterOperatorFactory {
    /**
     * Returns true when a filter can be generated without changing the schema.
     * Uses LONG columns to keep filter semantics stable across engine and oracle.
     */
    public boolean supports(RecordCursorFactory input) {
        return pickLongColumn(input.getMetadata(), new Rnd(0, 0)) != -1;
    }

    /**
     * Applies a deterministic filter so the oracle can mirror the exact predicate.
     */
    public OperatorState apply(FuzzBuildContext context, OperatorState state, Rnd rnd) throws SqlException {
        int columnIndex = pickLongColumn(state.underTest.getMetadata(), rnd);
        int threshold = rnd.nextInt(512);
        TestLongGreaterThanFunction filterUnderTest = new TestLongGreaterThanFunction(columnIndex, threshold);
        TestLongGreaterThanFunction filterOracle = new TestLongGreaterThanFunction(columnIndex, threshold);
        state.underTest = new FilteredRecordCursorFactory(state.underTest, filterUnderTest);
        state.oracle = new ReferenceFilterRecordCursorFactory(state.oracle, filterOracle);
        return state;
    }

    private int pickLongColumn(io.questdb.cairo.sql.RecordMetadata metadata, Rnd rnd) {
        int count = metadata.getColumnCount();
        int[] candidates = new int[count];
        int size = 0;
        for (int i = 0; i < count; i++) {
            if (metadata.getColumnType(i) == io.questdb.cairo.ColumnType.LONG) {
                candidates[size++] = i;
            }
        }
        if (size == 0) {
            return -1;
        }
        return candidates[rnd.nextInt(size)];
    }
}
