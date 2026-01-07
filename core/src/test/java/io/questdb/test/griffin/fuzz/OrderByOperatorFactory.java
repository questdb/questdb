package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;

public final class OrderByOperatorFactory {
    /**
     * Returns true when the input includes at least one orderable column.
     * This guards the fuzz run from generating non-deterministic comparisons.
     */
    public boolean supports(RecordCursorFactory input) {
        // Only enable if at least one column is orderable by the comparator stack.
        for (int i = 0, n = input.getMetadata().getColumnCount(); i < n; i++) {
            if (isOrderable(input.getMetadata().getColumnType(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Applies a deterministic ORDER BY and marks output as ordered for comparison.
     * This allows LIMIT and result equality checks to be repeatable.
     */
    public OperatorState apply(FuzzBuildContext context, OperatorState state, Rnd rnd) throws SqlException {
        int columnCount = state.underTest.getMetadata().getColumnCount();
        IntList orderable = new IntList();
        for (int i = 0; i < columnCount; i++) {
            if (isOrderable(state.underTest.getMetadata().getColumnType(i))) {
                orderable.add(i);
            }
        }
        // No orderable columns means ORDER BY isn't applicable for this input.
        if (orderable.size() == 0) {
            return state;
        }
        IntList orderByColumns = new IntList();
        boolean[] used = new boolean[columnCount];
        int first = orderable.getQuick(rnd.nextInt(orderable.size()));
        orderByColumns.add(first + 1);
        used[first] = true;
        if (orderable.size() > 1 && rnd.nextBoolean()) {
            int second = orderable.getQuick(rnd.nextInt(orderable.size()));
            while (second == first) {
                second = orderable.getQuick(rnd.nextInt(orderable.size()));
            }
            orderByColumns.add(second + 1);
            used[second] = true;
        }
        for (int i = 0; i < columnCount; i++) {
            if (!used[i] && isOrderable(state.underTest.getMetadata().getColumnType(i))) {
                orderByColumns.add(i + 1);
            }
        }
        state.underTest = OrderBySupport.orderBy(context, state.underTest, orderByColumns);
        state.oracle = new ReferenceOrderByRecordCursorFactory(state.oracle, orderByColumns);
        state.ordered = true;
        return state;
    }

    private static boolean isOrderable(int columnType) {
        // Exclude types that the comparator or ORDER BY pipeline doesn't support.
        return switch (io.questdb.cairo.ColumnType.tagOf(columnType)) {
            case io.questdb.cairo.ColumnType.BINARY,
                    io.questdb.cairo.ColumnType.ARRAY,
                    io.questdb.cairo.ColumnType.ARRAY_STRING,
                    io.questdb.cairo.ColumnType.CURSOR,
                    io.questdb.cairo.ColumnType.VAR_ARG,
                    io.questdb.cairo.ColumnType.RECORD,
                    io.questdb.cairo.ColumnType.PARAMETER,
                    io.questdb.cairo.ColumnType.INTERVAL,
                    io.questdb.cairo.ColumnType.NULL,
                    io.questdb.cairo.ColumnType.UNDEFINED,
                    io.questdb.cairo.ColumnType.GEOHASH -> false;
            default -> true;
        };
    }
}
