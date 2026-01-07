package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long128;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8s;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.StringSink;

import java.util.Arrays;
import java.util.Comparator;

final class ReferenceRowSet {
    private final RecordMetadata metadata;
    private final int columnCount;
    private final ObjList<Object[]> rows;
    private int rowCount;

    ReferenceRowSet(RecordMetadata metadata) {
        this.metadata = metadata;
        this.columnCount = metadata.getColumnCount();
        this.rows = new ObjList<>();
    }

    RecordMetadata getMetadata() {
        return metadata;
    }

    int size() {
        return rowCount;
    }

    Object getObject(int row, int col) {
        return rows.getQuick(row)[col];
    }

    Object[] getRow(int row) {
        return rows.getQuick(row);
    }

    void addRow(Record record) {
        Object[] row = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            int type = ColumnType.tagOf(columnType);
            switch (type) {
                case ColumnType.BOOLEAN:
                    row[i] = record.getBool(i);
                    break;
                case ColumnType.BYTE:
                    row[i] = record.getByte(i);
                    break;
                case ColumnType.SHORT:
                    row[i] = record.getShort(i);
                    break;
                case ColumnType.CHAR:
                    row[i] = record.getChar(i);
                    break;
                case ColumnType.INT:
                    row[i] = record.getInt(i);
                    break;
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                    row[i] = record.getLong(i);
                    break;
                case ColumnType.FLOAT:
                    row[i] = record.getFloat(i);
                    break;
                case ColumnType.DOUBLE:
                    row[i] = record.getDouble(i);
                    break;
                case ColumnType.STRING: {
                    CharSequence value = record.getStrA(i);
                    row[i] = value == null ? null : value.toString();
                    break;
                }
                case ColumnType.SYMBOL: {
                    CharSequence value = record.getSymA(i);
                    row[i] = value == null ? null : value.toString();
                    break;
                }
                case ColumnType.VARCHAR: {
                    Utf8Sequence value = record.getVarcharA(i);
                    row[i] = value == null ? null : Utf8String.newInstance(value);
                    break;
                }
                case ColumnType.BINARY: {
                    BinarySequence value = record.getBin(i);
                    row[i] = value == null ? null : new BinaryValue(value);
                    break;
                }
                case ColumnType.LONG256: {
                    Long256 value = record.getLong256A(i);
                    if (value == null) {
                        row[i] = Long256Impl.NULL_LONG256;
                    } else {
                        Long256Impl copy = new Long256Impl();
                        copy.copyFrom(value);
                        row[i] = copy;
                    }
                    break;
                }
                case ColumnType.UUID:
                case ColumnType.LONG128: {
                    row[i] = new Long128Value(record.getLong128Hi(i), record.getLong128Lo(i));
                    break;
                }
                case ColumnType.IPv4:
                    row[i] = record.getIPv4(i);
                    break;
                case ColumnType.GEOBYTE:
                    row[i] = record.getGeoByte(i);
                    break;
                case ColumnType.GEOSHORT:
                    row[i] = record.getGeoShort(i);
                    break;
                case ColumnType.GEOINT:
                    row[i] = record.getGeoInt(i);
                    break;
                case ColumnType.GEOLONG:
                    row[i] = record.getGeoLong(i);
                    break;
                case ColumnType.ARRAY: {
                    var array = record.getArray(i, columnType);
                    if (array == null || array.isNull()) {
                        row[i] = null;
                    } else {
                        StringSink sink = new StringSink();
                        ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE);
                        DoubleArrayParser parser = new DoubleArrayParser();
                        parser.of(sink, ColumnType.decodeArrayDimensionality(columnType));
                        row[i] = new ArrayValue(parser, sink.toString());
                    }
                    break;
                }
                case ColumnType.DECIMAL8:
                    row[i] = record.getDecimal8(i);
                    break;
                case ColumnType.DECIMAL16:
                    row[i] = record.getDecimal16(i);
                    break;
                case ColumnType.DECIMAL32:
                    row[i] = record.getDecimal32(i);
                    break;
                case ColumnType.DECIMAL64:
                    row[i] = record.getDecimal64(i);
                    break;
                case ColumnType.DECIMAL128: {
                    Decimal128 decimal = new Decimal128();
                    record.getDecimal128(i, decimal);
                    row[i] = decimal;
                    break;
                }
                case ColumnType.DECIMAL256: {
                    Decimal256 decimal = new Decimal256();
                    record.getDecimal256(i, decimal);
                    row[i] = decimal;
                    break;
                }
                default:
                    throw new UnsupportedOperationException("unsupported type in reference row set: " + ColumnType.nameOf(type));
            }
        }
        rows.add(row);
        rowCount++;
    }

    void addRow(Object[] row) {
        rows.add(row);
        rowCount++;
    }

    ReferenceRowSet slice(long lo, long hi) {
        if (lo < 0) {
            lo = 0;
        }
        if (hi < 0 || hi > rowCount) {
            hi = rowCount;
        }
        if (lo >= hi) {
            return new ReferenceRowSet(metadata);
        }
        ReferenceRowSet out = new ReferenceRowSet(metadata);
        int start = (int) lo;
        int end = (int) hi;
        for (int row = start; row < end; row++) {
            out.rows.add(rows.getQuick(row).clone());
            out.rowCount++;
        }
        return out;
    }

    ReferenceRowSet sorted(IntList orderByColumns) {
        Integer[] order = new Integer[rowCount];
        for (int i = 0; i < rowCount; i++) {
            order[i] = i;
        }
        Comparator<Integer> comparator = (a, b) -> compareRows(a, b, orderByColumns);
        Arrays.sort(order, comparator);
        ReferenceRowSet out = new ReferenceRowSet(metadata);
        for (int row : order) {
            out.rows.add(rows.getQuick(row).clone());
            out.rowCount++;
        }
        return out;
    }

    private int compareRows(int leftRow, int rightRow, IntList orderByColumns) {
        for (int i = 0, n = orderByColumns.size(); i < n; i++) {
            int key = orderByColumns.getQuick(i);
            int direction = key >= 0 ? 1 : -1;
            int col = Math.abs(key) - 1;
            int type = ColumnType.tagOf(metadata.getColumnType(col));
            int cmp = compareValues(type, getObject(leftRow, col), getObject(rightRow, col));
            if (cmp != 0) {
                return cmp * direction;
            }
        }
        return 0;
    }

    private static int compareValues(int type, Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        switch (type) {
            case ColumnType.BOOLEAN:
                return Boolean.compare((boolean) left, (boolean) right);
            case ColumnType.BYTE:
                return Byte.compare((byte) left, (byte) right);
            case ColumnType.SHORT:
                return Short.compare((short) left, (short) right);
            case ColumnType.CHAR:
                return Character.compare((char) left, (char) right);
            case ColumnType.INT:
                return Integer.compare((int) left, (int) right);
            case ColumnType.LONG:
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
                return Long.compare((long) left, (long) right);
            case ColumnType.FLOAT:
                return Numbers.compare((float) left, (float) right);
            case ColumnType.DOUBLE:
                return Numbers.compare((double) left, (double) right);
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                return ((String) left).compareTo((String) right);
            case ColumnType.VARCHAR:
                return Utf8s.compare((Utf8Sequence) left, (Utf8Sequence) right);
            case ColumnType.BINARY:
                return BinaryValue.compare((BinaryValue) left, (BinaryValue) right);
            case ColumnType.LONG256:
                return Long256Util.compare((Long256) left, (Long256) right);
            case ColumnType.UUID: {
                Long128Value l = (Long128Value) left;
                Long128Value r = (Long128Value) right;
                return Uuid.compare(l.hi, l.lo, r.hi, r.lo);
            }
            case ColumnType.LONG128: {
                Long128Value l = (Long128Value) left;
                Long128Value r = (Long128Value) right;
                return Long128.compare(l.hi, l.lo, r.hi, r.lo);
            }
            case ColumnType.IPv4:
                return Long.compare(Numbers.ipv4ToLong((int) left), Numbers.ipv4ToLong((int) right));
            case ColumnType.GEOBYTE:
                return Byte.compare((byte) left, (byte) right);
            case ColumnType.GEOSHORT:
                return Short.compare((short) left, (short) right);
            case ColumnType.GEOINT:
                return Integer.compare((int) left, (int) right);
            case ColumnType.GEOLONG:
                return Long.compare((long) left, (long) right);
            case ColumnType.ARRAY:
                return ((ArrayValue) left).text.compareTo(((ArrayValue) right).text);
            case ColumnType.DECIMAL8:
                return Byte.compare((byte) left, (byte) right);
            case ColumnType.DECIMAL16:
                return Short.compare((short) left, (short) right);
            case ColumnType.DECIMAL32:
                return Integer.compare((int) left, (int) right);
            case ColumnType.DECIMAL64:
                return Long.compare((long) left, (long) right);
            case ColumnType.DECIMAL128:
                return Decimal128.compare((Decimal128) left, (Decimal128) right);
            case ColumnType.DECIMAL256:
                return Decimal256.compare((Decimal256) left, (Decimal256) right);
            default:
                throw new UnsupportedOperationException("unsupported type in reference row set: " + ColumnType.nameOf(type));
        }
    }

    static ReferenceRowSet materialize(
            RecordCursorFactory base,
            RecordMetadata metadata,
            SqlExecutionContext sqlExecutionContext,
            RowPredicate predicate
    ) throws SqlException {
        ReferenceRowSet rowSet = new ReferenceRowSet(metadata);
        try (RecordCursor cursor = base.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (predicate == null || predicate.test(record)) {
                    rowSet.addRow(record);
                }
            }
        }
        return rowSet;
    }

    interface RowPredicate {
        boolean test(Record record);
    }

    static Object nullValue(int type) {
        // Use engine null sentinels so reference rows match built-in semantics.
        return switch (type) {
            case ColumnType.INT -> Numbers.INT_NULL;
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE -> Numbers.LONG_NULL;
            case ColumnType.FLOAT -> Float.NaN;
            case ColumnType.DOUBLE -> Double.NaN;
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR, ColumnType.BINARY, ColumnType.ARRAY -> null;
            case ColumnType.BOOLEAN -> false;
            case ColumnType.BYTE -> (byte) 0;
            case ColumnType.SHORT -> (short) 0;
            case ColumnType.CHAR -> (char) 0;
            case ColumnType.IPv4 -> Numbers.IPv4_NULL;
            case ColumnType.GEOBYTE -> GeoHashes.BYTE_NULL;
            case ColumnType.GEOSHORT -> GeoHashes.SHORT_NULL;
            case ColumnType.GEOINT -> GeoHashes.INT_NULL;
            case ColumnType.GEOLONG -> GeoHashes.NULL;
            case ColumnType.LONG256 -> Long256Impl.NULL_LONG256;
            case ColumnType.UUID, ColumnType.LONG128 -> new Long128Value(Numbers.LONG_NULL, Numbers.LONG_NULL);
            case ColumnType.DECIMAL8 -> io.questdb.std.Decimals.DECIMAL8_NULL;
            case ColumnType.DECIMAL16 -> io.questdb.std.Decimals.DECIMAL16_NULL;
            case ColumnType.DECIMAL32 -> io.questdb.std.Decimals.DECIMAL32_NULL;
            case ColumnType.DECIMAL64 -> io.questdb.std.Decimals.DECIMAL64_NULL;
            case ColumnType.DECIMAL128 -> Decimal128.NULL_VALUE;
            case ColumnType.DECIMAL256 -> Decimal256.NULL_VALUE;
            default -> null;
        };
    }

    static boolean isNull(int type, Object value) {
        // Match engine null checks across numeric and special types for join key evaluation.
        if (value == null) {
            return true;
        }
        return switch (type) {
            case ColumnType.INT -> (int) value == Numbers.INT_NULL;
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE -> (long) value == Numbers.LONG_NULL;
            case ColumnType.FLOAT -> Numbers.isNull((float) value);
            case ColumnType.DOUBLE -> Numbers.isNull((double) value);
            case ColumnType.IPv4 -> (int) value == Numbers.IPv4_NULL;
            case ColumnType.GEOBYTE -> (byte) value == GeoHashes.BYTE_NULL;
            case ColumnType.GEOSHORT -> (short) value == GeoHashes.SHORT_NULL;
            case ColumnType.GEOINT -> (int) value == GeoHashes.INT_NULL;
            case ColumnType.GEOLONG -> (long) value == GeoHashes.NULL;
            case ColumnType.LONG256 -> Long256Impl.isNull((Long256) value);
            case ColumnType.UUID, ColumnType.LONG128 -> {
                Long128Value v = (Long128Value) value;
                yield Long128.isNull(v.lo, v.hi);
            }
            case ColumnType.DECIMAL8 -> (byte) value == io.questdb.std.Decimals.DECIMAL8_NULL;
            case ColumnType.DECIMAL16 -> (short) value == io.questdb.std.Decimals.DECIMAL16_NULL;
            case ColumnType.DECIMAL32 -> (int) value == io.questdb.std.Decimals.DECIMAL32_NULL;
            case ColumnType.DECIMAL64 -> (long) value == io.questdb.std.Decimals.DECIMAL64_NULL;
            case ColumnType.DECIMAL128 -> ((Decimal128) value).isNull();
            case ColumnType.DECIMAL256 -> ((Decimal256) value).isNull();
            default -> false;
        };
    }

    static final class Cursor implements RecordCursor {
        private final ReferenceRowSet rowSet;
        private final RowRecord recordA;
        private final RowRecord recordB;
        private int row = -1;

        Cursor(ReferenceRowSet rowSet) {
            this.rowSet = rowSet;
            this.recordA = new RowRecord(rowSet);
            this.recordB = new RowRecord(rowSet);
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public boolean hasNext() {
            if (++row < rowSet.rowCount) {
                recordA.of(row);
                recordB.of(row);
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return rowSet.rowCount;
        }

        @Override
        public long size() {
            return rowSet.rowCount;
        }

        @Override
        public void toTop() {
            row = -1;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((RowRecord) record).of((int) atRowId);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }
    }

    static final class RowRecord implements Record {
        private final ReferenceRowSet rowSet;
        private int row = -1;

        RowRecord(ReferenceRowSet rowSet) {
            this.rowSet = rowSet;
        }

        void of(int row) {
            this.row = row;
        }

        @Override
        public long getRowId() {
            return row;
        }

        @Override
        public long getLong(int col) {
            return (long) rowSet.getObject(row, col);
        }

        @Override
        public int getInt(int col) {
            return (int) rowSet.getObject(row, col);
        }

        @Override
        public short getShort(int col) {
            return (short) rowSet.getObject(row, col);
        }

        @Override
        public byte getByte(int col) {
            return (byte) rowSet.getObject(row, col);
        }

        @Override
        public boolean getBool(int col) {
            return (boolean) rowSet.getObject(row, col);
        }

        @Override
        public char getChar(int col) {
            return (char) rowSet.getObject(row, col);
        }

        @Override
        public float getFloat(int col) {
            return (float) rowSet.getObject(row, col);
        }

        @Override
        public double getDouble(int col) {
            return (double) rowSet.getObject(row, col);
        }

        @Override
        public CharSequence getStrA(int col) {
            return (CharSequence) rowSet.getObject(row, col);
        }

        @Override
        public CharSequence getStrB(int col) {
            return (CharSequence) rowSet.getObject(row, col);
        }

        @Override
        public CharSequence getSymA(int col) {
            return (CharSequence) rowSet.getObject(row, col);
        }

        @Override
        public CharSequence getSymB(int col) {
            return (CharSequence) rowSet.getObject(row, col);
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return (Utf8Sequence) rowSet.getObject(row, col);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return (Utf8Sequence) rowSet.getObject(row, col);
        }

        @Override
        public BinarySequence getBin(int col) {
            return (BinarySequence) rowSet.getObject(row, col);
        }

        @Override
        public long getBinLen(int col) {
            BinarySequence seq = getBin(col);
            return seq == null ? -1 : seq.length();
        }

        @Override
        public Long256 getLong256A(int col) {
            return (Long256) rowSet.getObject(row, col);
        }

        @Override
        public Long256 getLong256B(int col) {
            return (Long256) rowSet.getObject(row, col);
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
            return ((Long128Value) rowSet.getObject(row, col)).hi;
        }

        @Override
        public long getLong128Lo(int col) {
            return ((Long128Value) rowSet.getObject(row, col)).lo;
        }

        @Override
        public int getIPv4(int col) {
            return (int) rowSet.getObject(row, col);
        }

        @Override
        public byte getGeoByte(int col) {
            return (byte) rowSet.getObject(row, col);
        }

        @Override
        public short getGeoShort(int col) {
            return (short) rowSet.getObject(row, col);
        }

        @Override
        public int getGeoInt(int col) {
            return (int) rowSet.getObject(row, col);
        }

        @Override
        public long getGeoLong(int col) {
            return (long) rowSet.getObject(row, col);
        }

        @Override
        public io.questdb.cairo.arr.ArrayView getArray(int col, int columnType) {
            Object value = rowSet.getObject(row, col);
            return value == null ? null : ((ArrayValue) value).array;
        }

        @Override
        public byte getDecimal8(int col) {
            return (byte) rowSet.getObject(row, col);
        }

        @Override
        public short getDecimal16(int col) {
            return (short) rowSet.getObject(row, col);
        }

        @Override
        public int getDecimal32(int col) {
            return (int) rowSet.getObject(row, col);
        }

        @Override
        public long getDecimal64(int col) {
            return (long) rowSet.getObject(row, col);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            sink.copyFrom((Decimal128) rowSet.getObject(row, col));
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.copyFrom((Decimal256) rowSet.getObject(row, col));
        }
    }

    static final class BinaryValue implements BinarySequence {
        private final byte[] bytes;

        BinaryValue(BinarySequence source) {
            // Materialize binary values to decouple from cursor reuse.
            int len = (int) source.length();
            this.bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = source.byteAt(i);
            }
        }

        @Override
        public byte byteAt(long index) {
            return bytes[(int) index];
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BinaryValue other)) {
                return false;
            }
            return Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        static int compare(BinaryValue left, BinaryValue right) {
            int min = Math.min(left.bytes.length, right.bytes.length);
            for (int i = 0; i < min; i++) {
                int diff = Byte.compare(left.bytes[i], right.bytes[i]);
                if (diff != 0) {
                    return diff;
                }
            }
            return Integer.compare(left.bytes.length, right.bytes.length);
        }
    }

    static final class Long128Value {
        final long hi;
        final long lo;

        Long128Value(long hi, long lo) {
            this.hi = hi;
            this.lo = lo;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Long128Value other)) {
                return false;
            }
            return hi == other.hi && lo == other.lo;
        }

        @Override
        public int hashCode() {
            long value = hi ^ lo;
            return (int) (value ^ (value >>> 32));
        }
    }

    static final class ArrayValue {
        final io.questdb.cairo.arr.ArrayView array;
        final String text;

        ArrayValue(io.questdb.cairo.arr.ArrayView array, String text) {
            // Keep both array view and JSON text for ordering and stable comparisons.
            this.array = array;
            this.text = text;
        }
    }
}
