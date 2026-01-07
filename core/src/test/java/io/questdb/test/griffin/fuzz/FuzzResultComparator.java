package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;

public final class FuzzResultComparator {
    private FuzzResultComparator() {
    }

    /**
     * Materializes both factories, normalizes ordering if needed, and asserts equality.
     */
    public static void assertEqual(
            FuzzBuildContext context,
            RecordCursorFactory underTest,
            RecordCursorFactory oracle,
            SqlExecutionContext executionContext,
            boolean ordered
    ) throws Exception {
        StringSink actualSink = new StringSink();
        StringSink expectedSink = new StringSink();

        try (RecordCursorFactory ignoredLeft = underTest;
             RecordCursorFactory ignoredRight = oracle) {
            ReferenceRowSet actual = ReferenceRowSet.materialize(underTest, underTest.getMetadata(), executionContext, null);
            ReferenceRowSet expected = ReferenceRowSet.materialize(oracle, oracle.getMetadata(), executionContext, null);
            if (!ordered) {
                IntList orderByColumns = new IntList(actual.getMetadata().getColumnCount());
                for (int i = 0; i < actual.getMetadata().getColumnCount(); i++) {
                    orderByColumns.add(i + 1);
                }
                actual = actual.sorted(orderByColumns);
                expected = expected.sorted(orderByColumns);
            }
            try (RecordCursor actualCursor = new ReferenceRowSet.Cursor(actual);
                 RecordCursor expectedCursor = new ReferenceRowSet.Cursor(expected)) {
                AbstractCairoTest.println(actual.getMetadata(), actualCursor, actualSink);
                AbstractCairoTest.println(expected.getMetadata(), expectedCursor, expectedSink);
            }
        }
        TestUtils.assertEquals(expectedSink, actualSink);
    }
}
