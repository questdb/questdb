package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.Utf8Sequence;

public final class ReferenceJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory master;
    private final RecordCursorFactory slave;
    private final int joinKind;
    private final JoinKeyPlan keyPlan;
    private final Function joinFilter;

    /**
     * Reference implementation for standard join kinds using materialized row sets.
     */
    public ReferenceJoinRecordCursorFactory(
            RecordCursorFactory master,
            RecordCursorFactory slave,
            RecordMetadata metadata,
            int joinKind,
            JoinKeyPlan keyPlan,
            Function joinFilter
    ) {
        super(metadata);
        this.master = master;
        this.slave = slave;
        this.joinKind = joinKind;
        this.keyPlan = keyPlan;
        this.joinFilter = joinFilter;
    }

    /**
     * Executes the reference join by materializing both sides and producing joined rows.
     */
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        ReferenceRowSet left = ReferenceRowSet.materialize(master, master.getMetadata(), executionContext, null);
        ReferenceRowSet right = ReferenceRowSet.materialize(slave, slave.getMetadata(), executionContext, null);

        ReferenceRowSet out = new ReferenceRowSet(getMetadata());
        boolean[] rightMatched = new boolean[right.size()];
        int leftCols = master.getMetadata().getColumnCount();
        int rightCols = slave.getMetadata().getColumnCount();

        ReferenceJoinRecord joinRecord = joinFilter == null ? null : new ReferenceJoinRecord(
                getMetadata(),
                master.getMetadata(),
                slave.getMetadata()
        );

        for (int i = 0, ln = left.size(); i < ln; i++) {
            boolean matched = false;
            for (int j = 0, rn = right.size(); j < rn; j++) {
                if (!keysMatch(left, right, i, j)) {
                    continue;
                }
                Object[] row = combineRows(left, right, i, j, leftCols, rightCols);
                if (joinFilter != null) {
                    joinRecord.of(row);
                    if (!joinFilter.getBool(joinRecord)) {
                        continue;
                    }
                }
                matched = true;
                rightMatched[j] = true;
                out.addRow(row);
            }
            if (!matched && (joinKind == QueryModel.JOIN_LEFT_OUTER || joinKind == QueryModel.JOIN_FULL_OUTER || joinKind == QueryModel.JOIN_CROSS_LEFT)) {
                Object[] row = combineRows(left, null, i, -1, leftCols, rightCols);
                out.addRow(row);
            }
        }

        if (joinKind == QueryModel.JOIN_RIGHT_OUTER || joinKind == QueryModel.JOIN_FULL_OUTER || joinKind == QueryModel.JOIN_CROSS_RIGHT || joinKind == QueryModel.JOIN_CROSS_FULL) {
            for (int j = 0, rn = right.size(); j < rn; j++) {
                if (rightMatched[j]) {
                    continue;
                }
                Object[] row = combineRows(null, right, -1, j, leftCols, rightCols);
                out.addRow(row);
            }
        }

        if (joinKind == QueryModel.JOIN_CROSS) {
            ReferenceRowSet cross = new ReferenceRowSet(getMetadata());
            for (int i = 0, ln = left.size(); i < ln; i++) {
                for (int j = 0, rn = right.size(); j < rn; j++) {
                    cross.addRow(combineRows(left, right, i, j, leftCols, rightCols));
                }
            }
            out = cross;
        }

        return new ReferenceRowSet.Cursor(out);
    }

    /**
     * Reference cursor is backed by a materialized row set and supports random access.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Emits a simple plan node so reference joins are identifiable in debug output.
     */
    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Reference Join");
        sink.child(master);
        sink.child(slave);
    }

    @Override
    protected void _close() {
        master.close();
        slave.close();
        if (joinFilter != null) {
            joinFilter.close();
        }
    }

    private boolean keysMatch(ReferenceRowSet left, ReferenceRowSet right, int leftRow, int rightRow) {
        if (keyPlan == null) {
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

    private Object[] combineRows(
            ReferenceRowSet left,
            ReferenceRowSet right,
            int leftRow,
            int rightRow,
            int leftCols,
            int rightCols
    ) {
        Object[] out = new Object[leftCols + rightCols];
        if (left != null) {
            Object[] leftValues = left.getRow(leftRow);
            System.arraycopy(leftValues, 0, out, 0, leftCols);
        } else {
            fillNulls(out, 0, leftCols, master.getMetadata());
        }
        if (right != null) {
            Object[] rightValues = right.getRow(rightRow);
            System.arraycopy(rightValues, 0, out, leftCols, rightCols);
        } else {
            fillNulls(out, leftCols, rightCols, slave.getMetadata());
        }
        return out;
    }

    private static void fillNulls(Object[] out, int offset, int count, RecordMetadata metadata) {
        for (int i = 0; i < count; i++) {
            int type = ColumnType.tagOf(metadata.getColumnType(i));
            out[offset + i] = ReferenceRowSet.nullValue(type);
        }
    }

    private static final class ReferenceJoinRecord implements Record {
        private final RecordMetadata metadata;
        private Object[] row;

        ReferenceJoinRecord(RecordMetadata metadata, RecordMetadata left, RecordMetadata right) {
            this.metadata = metadata;
        }

        ReferenceJoinRecord of(Object[] row) {
            this.row = row;
            return this;
        }

        @Override
        public boolean getBool(int col) {
            return (boolean) row[col];
        }

        @Override
        public byte getByte(int col) {
            return (byte) row[col];
        }

        @Override
        public short getShort(int col) {
            return (short) row[col];
        }

        @Override
        public int getInt(int col) {
            return (int) row[col];
        }

        @Override
        public long getLong(int col) {
            return (long) row[col];
        }

        @Override
        public float getFloat(int col) {
            return (float) row[col];
        }

        @Override
        public double getDouble(int col) {
            return (double) row[col];
        }

        @Override
        public char getChar(int col) {
            return (char) row[col];
        }

        @Override
        public CharSequence getStrA(int col) {
            return (CharSequence) row[col];
        }

        @Override
        public CharSequence getStrB(int col) {
            return (CharSequence) row[col];
        }

        @Override
        public CharSequence getSymA(int col) {
            return (CharSequence) row[col];
        }

        @Override
        public CharSequence getSymB(int col) {
            return (CharSequence) row[col];
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return (Utf8Sequence) row[col];
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return (Utf8Sequence) row[col];
        }

        @Override
        public BinarySequence getBin(int col) {
            return (BinarySequence) row[col];
        }

        @Override
        public long getBinLen(int col) {
            BinarySequence seq = getBin(col);
            return seq == null ? -1 : seq.length();
        }

        @Override
        public Long256 getLong256A(int col) {
            return (Long256) row[col];
        }

        @Override
        public Long256 getLong256B(int col) {
            return (Long256) row[col];
        }

        @Override
        public void getLong256(int col, io.questdb.std.str.CharSink<?> sink) {
            Long256 value = getLong256A(col);
            if (value == null) {
                return;
            }
            ((Long256Impl) value).toSink(sink);
        }

        @Override
        public long getLong128Hi(int col) {
            return ((ReferenceRowSet.Long128Value) row[col]).hi;
        }

        @Override
        public long getLong128Lo(int col) {
            return ((ReferenceRowSet.Long128Value) row[col]).lo;
        }

        @Override
        public int getIPv4(int col) {
            return (int) row[col];
        }

        @Override
        public byte getGeoByte(int col) {
            return (byte) row[col];
        }

        @Override
        public short getGeoShort(int col) {
            return (short) row[col];
        }

        @Override
        public int getGeoInt(int col) {
            return (int) row[col];
        }

        @Override
        public long getGeoLong(int col) {
            return (long) row[col];
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            Object value = row[col];
            return value == null ? null : ((ReferenceRowSet.ArrayValue) value).array;
        }

        @Override
        public byte getDecimal8(int col) {
            return (byte) row[col];
        }

        @Override
        public short getDecimal16(int col) {
            return (short) row[col];
        }

        @Override
        public int getDecimal32(int col) {
            return (int) row[col];
        }

        @Override
        public long getDecimal64(int col) {
            return (long) row[col];
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            sink.copyFrom((Decimal128) row[col]);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.copyFrom((Decimal256) row[col]);
        }
    }
}
