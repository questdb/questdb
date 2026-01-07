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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ReferenceSpliceJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory master;
    private final RecordCursorFactory slave;
    private final JoinKeyPlan keyPlan;

    /**
     * Reference SPLICE join that interleaves left/right rows by timestamp.
     */
    public ReferenceSpliceJoinRecordCursorFactory(
            RecordCursorFactory master,
            RecordCursorFactory slave,
            RecordMetadata metadata,
            JoinKeyPlan keyPlan
    ) {
        super(metadata);
        this.master = master;
        this.slave = slave;
        this.keyPlan = keyPlan;
    }

    /**
     * Builds the splice output by walking both inputs in timestamp order.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        ReferenceRowSet left = ReferenceRowSet.materialize(master, master.getMetadata(), executionContext, null);
        ReferenceRowSet right = ReferenceRowSet.materialize(slave, slave.getMetadata(), executionContext, null);

        int leftTsIndex = master.getMetadata().getTimestampIndex();
        int rightTsIndex = slave.getMetadata().getTimestampIndex();
        if (leftTsIndex == -1 || rightTsIndex == -1) {
            throw new IllegalStateException("SPLICE join requires timestamps");
        }

        int leftCols = master.getMetadata().getColumnCount();
        int rightCols = slave.getMetadata().getColumnCount();

        ReferenceRowSet out = new ReferenceRowSet(getMetadata());
        Map<JoinKey, Integer> lastLeft = new HashMap<>();
        Map<JoinKey, Integer> lastRight = new HashMap<>();

        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < left.size() || rightIndex < right.size()) {
            long leftTs = leftIndex < left.size() ? (long) left.getRow(leftIndex)[leftTsIndex] : Long.MAX_VALUE;
            long rightTs = rightIndex < right.size() ? (long) right.getRow(rightIndex)[rightTsIndex] : Long.MAX_VALUE;
            if (leftTs < rightTs) {
                processLeft(left, right, leftCols, rightCols, leftIndex, lastLeft, lastRight, out);
                leftIndex++;
            } else if (rightTs < leftTs) {
                processRight(left, right, leftCols, rightCols, rightIndex, lastLeft, lastRight, out);
                rightIndex++;
            } else {
                if (leftTs == Long.MAX_VALUE) {
                    break;
                }
                JoinKey leftKey = toKey(left, leftIndex, true);
                JoinKey rightKey = toKey(right, rightIndex, false);
                if (leftKey != null && leftKey.equals(rightKey)) {
                    lastLeft.put(leftKey, leftIndex);
                    lastRight.put(rightKey, rightIndex);
                    out.addRow(combineRows(left, right, leftIndex, rightIndex, leftCols, rightCols));
                    leftIndex++;
                    rightIndex++;
                } else {
                    processLeft(left, right, leftCols, rightCols, leftIndex, lastLeft, lastRight, out);
                    leftIndex++;
                    processRight(left, right, leftCols, rightCols, rightIndex, lastLeft, lastRight, out);
                    rightIndex++;
                }
            }
        }

        return new ReferenceRowSet.Cursor(out);
    }

    /**
     * Reference output is materialized and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a plan marker for debugging reference splice joins.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Splice Join");
        sink.child(master);
        sink.child(slave);
    }

    @Override
    protected void _close() {
        master.close();
        slave.close();
    }

    private void processLeft(
            ReferenceRowSet left,
            ReferenceRowSet right,
            int leftCols,
            int rightCols,
            int leftIndex,
            Map<JoinKey, Integer> lastLeft,
            Map<JoinKey, Integer> lastRight,
            ReferenceRowSet out
    ) {
        JoinKey key = toKey(left, leftIndex, true);
        if (key != null) {
            lastLeft.put(key, leftIndex);
        }
        Integer match = key == null ? null : lastRight.get(key);
        int rightIndex = match == null ? -1 : match;
        out.addRow(combineRows(left, right, leftIndex, rightIndex, leftCols, rightCols));
    }

    private void processRight(
            ReferenceRowSet left,
            ReferenceRowSet right,
            int leftCols,
            int rightCols,
            int rightIndex,
            Map<JoinKey, Integer> lastLeft,
            Map<JoinKey, Integer> lastRight,
            ReferenceRowSet out
    ) {
        JoinKey key = toKey(right, rightIndex, false);
        if (key != null) {
            lastRight.put(key, rightIndex);
        }
        Integer match = key == null ? null : lastLeft.get(key);
        int leftIndex = match == null ? -1 : match;
        out.addRow(combineRows(left, right, leftIndex, rightIndex, leftCols, rightCols));
    }

    private JoinKey toKey(ReferenceRowSet rowSet, int row, boolean isLeft) {
        if (keyPlan == null || keyPlan.masterKeyColumns.getColumnCount() == 0) {
            return JoinKey.EMPTY;
        }
        Object[] values = new Object[keyPlan.masterKeyColumns.getColumnCount()];
        for (int i = 0, n = keyPlan.masterKeyColumns.getColumnCount(); i < n; i++) {
            int col = isLeft
                    ? keyPlan.masterKeyColumns.getColumnIndexFactored(i)
                    : keyPlan.slaveKeyColumns.getColumnIndexFactored(i);
            int type = ColumnType.tagOf(isLeft ? master.getMetadata().getColumnType(col) : slave.getMetadata().getColumnType(col));
            Object value = rowSet.getObject(row, col);
            if (ReferenceRowSet.isNull(type, value)) {
                return null;
            }
            values[i] = value;
        }
        return new JoinKey(values);
    }

    private Object[] combineRows(
            ReferenceRowSet left,
            ReferenceRowSet right,
            int leftRow,
            int rightRow,
            int leftCols,
            int rightCols
    ) {
        Object[] out = new Object[leftCols + rightCols];
        if (leftRow >= 0) {
            System.arraycopy(left.getRow(leftRow), 0, out, 0, leftCols);
        } else {
            fillNulls(out, 0, leftCols, master.getMetadata());
        }
        if (rightRow >= 0) {
            System.arraycopy(right.getRow(rightRow), 0, out, leftCols, rightCols);
        } else {
            fillNulls(out, leftCols, rightCols, slave.getMetadata());
        }
        return out;
    }

    private static final class JoinKey {
        static final JoinKey EMPTY = new JoinKey(new Object[0]);
        private final Object[] values;
        private final int hash;

        JoinKey(Object[] values) {
            this.values = values;
            this.hash = Arrays.hashCode(values);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JoinKey other = (JoinKey) obj;
            return Arrays.equals(values, other.values);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return Objects.toString(values);
        }
    }

    private static void fillNulls(Object[] out, int offset, int count, RecordMetadata metadata) {
        for (int i = 0; i < count; i++) {
            int type = ColumnType.tagOf(metadata.getColumnType(i));
            out[offset + i] = ReferenceRowSet.nullValue(type);
        }
    }

}
