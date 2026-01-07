package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;

public final class ReferenceAsofJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory master;
    private final RecordCursorFactory slave;
    private final JoinKeyPlan keyPlan;
    private final long toleranceInterval;
    private final boolean strictLt;

    /**
     * Reference ASOF/LT join that materializes both sides and applies time-based matching.
     */
    public ReferenceAsofJoinRecordCursorFactory(
            RecordCursorFactory master,
            RecordCursorFactory slave,
            RecordMetadata metadata,
            JoinKeyPlan keyPlan,
            long toleranceInterval,
            boolean strictLt
    ) {
        super(metadata);
        this.master = master;
        this.slave = slave;
        this.keyPlan = keyPlan;
        this.toleranceInterval = toleranceInterval;
        this.strictLt = strictLt;
    }

    /**
     * Produces the ASOF/LT join output by scanning left rows and finding best right matches.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        ReferenceRowSet left = ReferenceRowSet.materialize(master, master.getMetadata(), executionContext, null);
        ReferenceRowSet right = ReferenceRowSet.materialize(slave, slave.getMetadata(), executionContext, null);

        int leftTsIndex = master.getMetadata().getTimestampIndex();
        int rightTsIndex = slave.getMetadata().getTimestampIndex();
        if (leftTsIndex == -1 || rightTsIndex == -1) {
            throw new IllegalStateException("ASOF/LT join requires timestamps");
        }

        ReferenceRowSet out = new ReferenceRowSet(getMetadata());
        int leftCols = master.getMetadata().getColumnCount();
        int rightCols = slave.getMetadata().getColumnCount();
        for (int i = 0, ln = left.size(); i < ln; i++) {
            Object[] leftRow = left.getRow(i);
            long leftTs = (long) leftRow[leftTsIndex];
            int bestIndex = -1;
            long bestTs = Long.MIN_VALUE;
            for (int j = 0, rn = right.size(); j < rn; j++) {
                Object[] rightRow = right.getRow(j);
                if (!keysMatch(left, right, i, j)) {
                    continue;
                }
                long rightTs = (long) rightRow[rightTsIndex];
                boolean tsOk = strictLt ? rightTs < leftTs : rightTs <= leftTs;
                if (!tsOk) {
                    continue;
                }
                if (toleranceInterval != Numbers.LONG_NULL && leftTs - rightTs > toleranceInterval) {
                    continue;
                }
                if (rightTs >= bestTs) {
                    bestTs = rightTs;
                    bestIndex = j;
                }
            }
            Object[] outRow = new Object[leftCols + rightCols];
            System.arraycopy(leftRow, 0, outRow, 0, leftCols);
            if (bestIndex != -1) {
                System.arraycopy(right.getRow(bestIndex), 0, outRow, leftCols, rightCols);
            } else {
                fillNulls(outRow, leftCols, rightCols, slave.getMetadata());
            }
            out.addRow(outRow);
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
     * Emits a plan marker for debugging reference ASOF/LT joins.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type(strictLt ? "Reference LT Join" : "Reference ASOF Join");
        sink.child(master);
        sink.child(slave);
    }

    @Override
    protected void _close() {
        master.close();
        slave.close();
    }

    private boolean keysMatch(ReferenceRowSet left, ReferenceRowSet right, int leftRow, int rightRow) {
        if (keyPlan == null || keyPlan.masterKeyColumns.getColumnCount() == 0) {
            return true;
        }
        for (int i = 0, n = keyPlan.masterKeyColumns.getColumnCount(); i < n; i++) {
            int masterCol = keyPlan.masterKeyColumns.getColumnIndexFactored(i);
            int slaveCol = keyPlan.slaveKeyColumns.getColumnIndexFactored(i);
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

    private static void fillNulls(Object[] out, int offset, int count, RecordMetadata metadata) {
        for (int i = 0; i < count; i++) {
            int type = ColumnType.tagOf(metadata.getColumnType(i));
            out[offset + i] = ReferenceRowSet.nullValue(type);
        }
    }
}
