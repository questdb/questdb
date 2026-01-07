package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

public final class ReferenceWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory master;
    private final RecordCursorFactory slave;
    private final WindowSpec windowSpec;
    private final IntList leftKeys;
    private final IntList rightKeys;

    /**
     * Reference WINDOW join that materializes inputs and computes window aggregates on the right side.
     */
    public ReferenceWindowJoinRecordCursorFactory(
            RecordCursorFactory master,
            RecordCursorFactory slave,
            RecordMetadata metadata,
            WindowSpec windowSpec,
            IntList leftKeys,
            IntList rightKeys
    ) {
        super(metadata);
        this.master = master;
        this.slave = slave;
        this.windowSpec = windowSpec;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
    }

    /**
     * Produces window join output by scanning left rows and counting matches in the time window.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        ReferenceRowSet left = ReferenceRowSet.materialize(master, master.getMetadata(), executionContext, null);
        ReferenceRowSet right = ReferenceRowSet.materialize(slave, slave.getMetadata(), executionContext, null);

        int leftTsIndex = master.getMetadata().getTimestampIndex();
        int rightTsIndex = slave.getMetadata().getTimestampIndex();
        if (leftTsIndex == -1 || rightTsIndex == -1) {
            throw new IllegalStateException("WINDOW join requires timestamps");
        }

        int leftCols = master.getMetadata().getColumnCount();
        int aggCount = windowSpec.groupByFunctions.size();
        ReferenceRowSet out = new ReferenceRowSet(getMetadata());
        for (int i = 0, ln = left.size(); i < ln; i++) {
            Object[] leftRow = left.getRow(i);
            long leftTs = (long) leftRow[leftTsIndex];
            long from = leftTs - windowSpec.lo;
            long to = leftTs + windowSpec.hi;
            long count = 0;

            for (int j = 0, rn = right.size(); j < rn; j++) {
                Object[] rightRow = right.getRow(j);
                long rightTs = (long) rightRow[rightTsIndex];
                if (rightTs < from || rightTs > to) {
                    continue;
                }
                if (!keysMatch(left, right, i, j)) {
                    continue;
                }
                count++;
            }
            Object[] row = new Object[leftCols + aggCount];
            System.arraycopy(leftRow, 0, row, 0, leftCols);
            for (int a = 0; a < aggCount; a++) {
                row[leftCols + a] = count == 0 ? Numbers.LONG_NULL : count;
            }
            out.addRow(row);
        }
        return new ReferenceRowSet.Cursor(out);
    }

    /**
     * Reference output is fully materialized and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a plan marker for debugging reference window joins.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Window Join");
        sink.child(master);
        sink.child(slave);
    }

    @Override
    protected void _close() {
        master.close();
        slave.close();
    }

    private boolean keysMatch(ReferenceRowSet left, ReferenceRowSet right, int leftRow, int rightRow) {
        if (leftKeys == null || leftKeys.size() == 0) {
            return true;
        }
        for (int i = 0, n = leftKeys.size(); i < n; i++) {
            int masterCol = leftKeys.getQuick(i);
            int slaveCol = rightKeys.getQuick(i);
            int type = ColumnType.tagOf(master.getMetadata().getColumnType(masterCol));
            Object leftValue = left.getObject(leftRow, masterCol);
            Object rightValue = right.getObject(rightRow, slaveCol);
            if (ReferenceRowSet.isNull(type, leftValue) || ReferenceRowSet.isNull(type, rightValue)) {
                return false;
            }
            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNull(int type, Object value) {
        return ReferenceRowSet.isNull(type, value);
    }
}
