/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

/**
 * Column-major accumulator for one QWP egress {@code RESULT_BATCH} table block.
 * <p>
 * Phase-1 design tradeoff: row accumulation uses heap-boxed values per row
 * (simple, correct, allocates). The emit path serialises column-by-column
 * into a caller-owned native wire buffer so the outbound frame is zero-copy
 * from the wire buffer to the socket.
 * <p>
 * One instance is reused across batches on the same connection.
 */
public class QwpResultBatchBuffer implements QuietCloseable {

    /**
     * Sentinel placeholder for NULL entries in the per-column value list.
     */
    private static final Object NULL = new Object();
    // reusable scratch for variable-length column byte serialisation built up row-by-row
    private final ObjList<byte[]> byteScratch = new ObjList<>();
    // columns[ci]: list of boxed values (Object = NULL sentinel, or type-specific box)
    private final ObjList<ObjList<Object>> columnValues = new ObjList<>();
    // per-column dicts for SYMBOL, built during append, emitted on flush
    private final ObjList<CharSequenceIntHashMap> symbolDict = new ObjList<>();
    private final ObjList<ObjList<String>> symbolDictOrder = new ObjList<>();
    private ObjList<QwpEgressColumnDef> columns;
    private int columnCount;
    private int rowCount;

    public QwpResultBatchBuffer() {
    }

    /**
     * Starts a new batch with the given schema. Clears per-column value lists.
     */
    public void beginBatch(ObjList<QwpEgressColumnDef> columns) {
        this.columns = columns;
        this.columnCount = columns.size();
        this.rowCount = 0;
        while (columnValues.size() < columnCount) {
            columnValues.add(new ObjList<>());
            symbolDict.add(new CharSequenceIntHashMap());
            symbolDictOrder.add(new ObjList<>());
            byteScratch.add(null);
        }
        for (int i = 0; i < columnCount; i++) {
            columnValues.getQuick(i).clear();
            symbolDict.getQuick(i).clear();
            symbolDictOrder.getQuick(i).clear();
        }
    }

    @Override
    public void close() {
        reset();
        columnValues.clear();
        symbolDict.clear();
        symbolDictOrder.clear();
        byteScratch.clear();
    }

    /**
     * Appends one row's worth of values from the given record, dispatching per column wire type.
     */
    public void appendRow(Record record) {
        for (int ci = 0; ci < columnCount; ci++) {
            QwpEgressColumnDef def = columns.getQuick(ci);
            ObjList<Object> values = columnValues.getQuick(ci);
            byte wire = def.getWireType();
            int qdbType = def.getQuestdbColumnType();
            switch (wire) {
                case QwpConstants.TYPE_BOOLEAN -> values.add(record.getBool(ci) ? Boolean.TRUE : Boolean.FALSE);
                case QwpConstants.TYPE_BYTE -> values.add((long) record.getByte(ci));
                case QwpConstants.TYPE_SHORT -> values.add((long) record.getShort(ci));
                case QwpConstants.TYPE_CHAR -> values.add((long) record.getChar(ci));
                case QwpConstants.TYPE_INT -> {
                    int v = record.getInt(ci);
                    values.add(v == Numbers.INT_NULL ? NULL : (long) v);
                }
                case QwpConstants.TYPE_LONG -> {
                    long v = record.getLong(ci);
                    values.add(v == Numbers.LONG_NULL ? NULL : v);
                }
                case QwpConstants.TYPE_DATE -> {
                    long v = record.getDate(ci);
                    values.add(v == Numbers.LONG_NULL ? NULL : v);
                }
                case QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS -> {
                    long v = record.getTimestamp(ci);
                    values.add(v == Numbers.LONG_NULL ? NULL : v);
                }
                case QwpConstants.TYPE_FLOAT -> {
                    float v = record.getFloat(ci);
                    values.add(Float.isNaN(v) ? NULL : v);
                }
                case QwpConstants.TYPE_DOUBLE -> {
                    double v = record.getDouble(ci);
                    values.add(Double.isNaN(v) ? NULL : v);
                }
                case QwpConstants.TYPE_STRING -> {
                    CharSequence cs = record.getStrA(ci);
                    values.add(cs == null ? NULL : cs.toString());
                }
                case QwpConstants.TYPE_VARCHAR -> {
                    Utf8Sequence us = record.getVarcharA(ci);
                    values.add(us == null ? NULL : copyUtf8(us));
                }
                case QwpConstants.TYPE_SYMBOL -> {
                    CharSequence cs = record.getSymA(ci);
                    if (cs == null) {
                        values.add(NULL);
                    } else {
                        String s = cs.toString();
                        CharSequenceIntHashMap dict = symbolDict.getQuick(ci);
                        int id = dict.get(s);
                        if (id == -1) {
                            id = symbolDictOrder.getQuick(ci).size();
                            dict.put(s, id);
                            symbolDictOrder.getQuick(ci).add(s);
                        }
                        values.add((long) id);
                    }
                }
                case QwpConstants.TYPE_UUID -> {
                    long lo = record.getLong128Lo(ci);
                    long hi = record.getLong128Hi(ci);
                    // Null UUID is both halves == LONG_NULL
                    if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                        values.add(NULL);
                    } else {
                        values.add(new long[]{lo, hi});
                    }
                }
                case QwpConstants.TYPE_LONG256 -> {
                    Long256 l256 = record.getLong256A(ci);
                    if (l256 == null || (l256.getLong0() == Numbers.LONG_NULL
                            && l256.getLong1() == Numbers.LONG_NULL
                            && l256.getLong2() == Numbers.LONG_NULL
                            && l256.getLong3() == Numbers.LONG_NULL)) {
                        values.add(NULL);
                    } else {
                        values.add(new long[]{l256.getLong0(), l256.getLong1(), l256.getLong2(), l256.getLong3()});
                    }
                }
                case QwpConstants.TYPE_GEOHASH -> {
                    long bits = readGeoBits(record, ci, def.getPrecisionBits(), qdbType);
                    // QuestDB uses -1 as the universal geohash NULL sentinel across all widths;
                    // when the narrower sentinel (BYTE_NULL = -1, SHORT_NULL = -1, INT_NULL = -1)
                    // is widened to long it sign-extends to -1L.
                    if (bits == -1L) {
                        values.add(NULL);
                    } else {
                        values.add(bits);
                    }
                }
                case QwpConstants.TYPE_DECIMAL64 -> {
                    long v = record.getDecimal64(ci);
                    values.add(v == Numbers.LONG_NULL ? NULL : v);
                }
                case QwpConstants.TYPE_DECIMAL128 -> {
                    Decimal128 sink = new Decimal128();
                    record.getDecimal128(ci, sink);
                    if (sink.isNull()) {
                        values.add(NULL);
                    } else {
                        // Snapshot: Decimal128 is mutable, so clone the two longs
                        values.add(new long[]{sink.getLow(), sink.getHigh()});
                    }
                }
                case QwpConstants.TYPE_DECIMAL256 -> {
                    Decimal256 sink = new Decimal256();
                    record.getDecimal256(ci, sink);
                    if (sink.isNull()) {
                        values.add(NULL);
                    } else {
                        values.add(new long[]{sink.getLl(), sink.getLh(), sink.getHl(), sink.getHh()});
                    }
                }
                case QwpConstants.TYPE_DOUBLE_ARRAY, QwpConstants.TYPE_LONG_ARRAY -> {
                    ArrayView av = record.getArray(ci, qdbType);
                    if (av == null || av.isNull()) {
                        values.add(NULL);
                    } else {
                        values.add(serialiseArray(av, wire));
                    }
                }
                default -> throw new UnsupportedOperationException(
                        "QWP egress append: unsupported wire type 0x" + Integer.toHexString(wire & 0xFF));
            }
        }
        rowCount++;
    }

    /**
     * Emits the full table block body for this batch starting at {@code wireBuf}:
     * name_length (=0) + row_count + column_count + schema + column data sections.
     *
     * @param wireBuf         native buffer start
     * @param wireLimit       exclusive end of the caller-owned buffer
     * @param schemaId        server-assigned schema id
     * @param writeFullSchema true for first batch of a query, false for subsequent references
     * @return number of bytes written, or -1 if the data would overflow wireLimit
     */
    public int emitTableBlock(long wireBuf, long wireLimit, long schemaId, boolean writeFullSchema) {
        long p = wireBuf;
        if (p >= wireLimit) return -1;
        // Anonymous result-set: empty name
        Unsafe.getUnsafe().putByte(p++, (byte) 0); // name_length = 0
        p = QwpVarint.encode(p, rowCount);
        p = QwpVarint.encode(p, columnCount);
        // Schema
        if (writeFullSchema) {
            int needed = QwpEgressSchemaWriter.worstCaseFullSize(columns);
            if (p + needed > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeFull(p, schemaId, columns);
        } else {
            if (p + 1 + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeReference(p, schemaId);
        }
        // Columns
        for (int ci = 0; ci < columnCount; ci++) {
            p = emitColumn(ci, p, wireLimit);
            if (p < 0) return -1;
        }
        return (int) (p - wireBuf);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void reset() {
        for (int i = 0, n = columnValues.size(); i < n; i++) {
            columnValues.getQuick(i).clear();
            symbolDict.getQuick(i).clear();
            symbolDictOrder.getQuick(i).clear();
        }
        rowCount = 0;
        columnCount = 0;
        columns = null;
    }

    private static byte[] copyUtf8(Utf8Sequence us) {
        int n = us.size();
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
            out[i] = us.byteAt(i);
        }
        return out;
    }

    private static long readGeoBits(Record record, int col, int precisionBits, int questdbColumnType) {
        short tag = ColumnType.tagOf(questdbColumnType);
        // Geohash tags: GEOBYTE, GEOSHORT, GEOINT, GEOLONG (sized by precision)
        if (precisionBits <= 7) return record.getGeoByte(col);
        if (precisionBits <= 15) return record.getGeoShort(col);
        if (precisionBits <= 31) return record.getGeoInt(col);
        return record.getGeoLong(col);
    }

    /**
     * Serialises an {@link ArrayView} to the per-row wire form:
     * [nDims u8] [dim lengths: nDims x i32 LE] [values: product(dims) x (8 bytes LE)].
     */
    private static byte[] serialiseArray(ArrayView av, byte wireType) {
        int nDims = av.getDimCount();
        int total = 1 + 4 * nDims;
        int elements = 1;
        for (int d = 0; d < nDims; d++) {
            elements *= av.getDimLen(d);
        }
        int valueSize = wireType == QwpConstants.TYPE_DOUBLE_ARRAY ? 8 : 8;
        total += elements * valueSize;
        byte[] out = new byte[total];
        int p = 0;
        out[p++] = (byte) nDims;
        for (int d = 0; d < nDims; d++) {
            int len = av.getDimLen(d);
            out[p++] = (byte) len;
            out[p++] = (byte) (len >>> 8);
            out[p++] = (byte) (len >>> 16);
            out[p++] = (byte) (len >>> 24);
        }
        if (wireType == QwpConstants.TYPE_DOUBLE_ARRAY) {
            for (int i = 0; i < elements; i++) {
                long bits = Double.doubleToRawLongBits(av.getDouble(i));
                putLongLE(out, p, bits);
                p += 8;
            }
        } else {
            for (int i = 0; i < elements; i++) {
                long v = av.getLong(i);
                putLongLE(out, p, v);
                p += 8;
            }
        }
        return out;
    }

    private static long writeNullBitmap(long wireBuf, ObjList<Object> values, int rowCount) {
        int nullCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (values.getQuick(i) == NULL) nullCount++;
        }
        if (nullCount == 0) {
            Unsafe.getUnsafe().putByte(wireBuf, (byte) 0);
            return wireBuf + 1;
        }
        Unsafe.getUnsafe().putByte(wireBuf, (byte) 1);
        long p = wireBuf + 1;
        int bytes = (rowCount + 7) >>> 3;
        // Zero the bitmap region first
        for (int b = 0; b < bytes; b++) Unsafe.getUnsafe().putByte(p + b, (byte) 0);
        for (int i = 0; i < rowCount; i++) {
            if (values.getQuick(i) == NULL) {
                long addr = p + (i >>> 3);
                byte cur = Unsafe.getUnsafe().getByte(addr);
                Unsafe.getUnsafe().putByte(addr, (byte) (cur | (1 << (i & 7))));
            }
        }
        return p + bytes;
    }

    private static void putLongLE(byte[] out, int off, long v) {
        out[off] = (byte) v;
        out[off + 1] = (byte) (v >>> 8);
        out[off + 2] = (byte) (v >>> 16);
        out[off + 3] = (byte) (v >>> 24);
        out[off + 4] = (byte) (v >>> 32);
        out[off + 5] = (byte) (v >>> 40);
        out[off + 6] = (byte) (v >>> 48);
        out[off + 7] = (byte) (v >>> 56);
    }

    /**
     * Emits one column's section (null flag + bitmap + values). Returns new wire position,
     * or -1 on overflow (computed conservatively at entry and by each sub-step).
     */
    private long emitColumn(int ci, long wireBuf, long wireLimit) {
        QwpEgressColumnDef def = columns.getQuick(ci);
        byte wire = def.getWireType();
        ObjList<Object> values = columnValues.getQuick(ci);
        // Worst-case headroom check: null_flag + bitmap
        long worstNull = 1 + ((rowCount + 7) >>> 3);
        if (wireBuf + worstNull > wireLimit) return -1;
        long p = writeNullBitmap(wireBuf, values, rowCount);
        return switch (wire) {
            case QwpConstants.TYPE_BOOLEAN -> emitBoolean(values, p, wireLimit);
            case QwpConstants.TYPE_BYTE -> emitFixedWidth(values, p, wireLimit, 1);
            case QwpConstants.TYPE_SHORT, QwpConstants.TYPE_CHAR -> emitFixedWidth(values, p, wireLimit, 2);
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_FLOAT -> emitFixedWidth(values, p, wireLimit, 4);
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DOUBLE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS, QwpConstants.TYPE_DATE -> emitFixedWidth(values, p, wireLimit, 8);
            case QwpConstants.TYPE_DECIMAL64 -> emitDecimal(values, def.getScale(), p, wireLimit, 8);
            case QwpConstants.TYPE_UUID -> emitLongPair(values, p, wireLimit);
            case QwpConstants.TYPE_DECIMAL128 -> emitDecimalPair(values, def.getScale(), p, wireLimit);
            case QwpConstants.TYPE_LONG256 -> emitLongQuad(values, p, wireLimit);
            case QwpConstants.TYPE_DECIMAL256 -> emitDecimalQuad(values, def.getScale(), p, wireLimit);
            case QwpConstants.TYPE_STRING, QwpConstants.TYPE_VARCHAR -> emitStringOrVarchar(values, wire, p, wireLimit);
            case QwpConstants.TYPE_SYMBOL -> emitSymbol(ci, p, wireLimit);
            case QwpConstants.TYPE_GEOHASH -> emitGeohash(values, def.getPrecisionBits(), p, wireLimit);
            case QwpConstants.TYPE_DOUBLE_ARRAY, QwpConstants.TYPE_LONG_ARRAY -> emitArray(values, p, wireLimit);
            default ->
                    throw new UnsupportedOperationException("QWP egress emit: unsupported wire type 0x" + Integer.toHexString(wire & 0xFF));
        };
    }

    private long emitDecimal(ObjList<Object> values, int scale, long p, long wireLimit, int sizeBytes) {
        if (p + 1 > wireLimit) return -1;
        Unsafe.getUnsafe().putByte(p++, (byte) scale);
        return emitFixedWidth(values, p, wireLimit, sizeBytes);
    }

    private long emitDecimalPair(ObjList<Object> values, int scale, long p, long wireLimit) {
        if (p + 1 > wireLimit) return -1;
        Unsafe.getUnsafe().putByte(p++, (byte) scale);
        return emitLongPair(values, p, wireLimit);
    }

    private long emitDecimalQuad(ObjList<Object> values, int scale, long p, long wireLimit) {
        if (p + 1 > wireLimit) return -1;
        Unsafe.getUnsafe().putByte(p++, (byte) scale);
        return emitLongQuad(values, p, wireLimit);
    }

    private long emitArray(ObjList<Object> values, long p, long wireLimit) {
        // Write scale prefix? No — arrays don't have scale. Precede with... per spec, just per-row bytes.
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            byte[] raw = (byte[]) v;
            if (p + raw.length > wireLimit) return -1;
            for (byte b : raw) Unsafe.getUnsafe().putByte(p++, b);
        }
        return p;
    }

    private long emitBoolean(ObjList<Object> values, long p, long wireLimit) {
        // Count non-null values; pack 8 per byte
        int nonNull = 0;
        for (int i = 0; i < rowCount; i++) if (values.getQuick(i) != NULL) nonNull++;
        int bytes = (nonNull + 7) >>> 3;
        if (p + bytes > wireLimit) return -1;
        for (int b = 0; b < bytes; b++) Unsafe.getUnsafe().putByte(p + b, (byte) 0);
        int bitIdx = 0;
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            if ((Boolean) v) {
                long addr = p + (bitIdx >>> 3);
                byte cur = Unsafe.getUnsafe().getByte(addr);
                Unsafe.getUnsafe().putByte(addr, (byte) (cur | (1 << (bitIdx & 7))));
            }
            bitIdx++;
        }
        return p + bytes;
    }

    private long emitFixedWidth(ObjList<Object> values, long p, long wireLimit, int sizeBytes) {
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            long bits = asLongBits(v, sizeBytes);
            if (p + sizeBytes > wireLimit) return -1;
            switch (sizeBytes) {
                case 1 -> Unsafe.getUnsafe().putByte(p, (byte) bits);
                case 2 -> Unsafe.getUnsafe().putShort(p, (short) bits);
                case 4 -> Unsafe.getUnsafe().putInt(p, (int) bits);
                case 8 -> Unsafe.getUnsafe().putLong(p, bits);
                default -> throw new IllegalStateException();
            }
            p += sizeBytes;
        }
        return p;
    }

    private long emitGeohash(ObjList<Object> values, int precisionBits, long p, long wireLimit) {
        // Precision varint first, then packed per-value
        if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
        p = QwpVarint.encode(p, precisionBits);
        int bytesPerValue = (precisionBits + 7) >>> 3;
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            long bits = (Long) v;
            if (p + bytesPerValue > wireLimit) return -1;
            for (int b = 0; b < bytesPerValue; b++) {
                Unsafe.getUnsafe().putByte(p++, (byte) (bits >>> (b * 8)));
            }
        }
        return p;
    }

    private long emitLongPair(ObjList<Object> values, long p, long wireLimit) {
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            long[] pair = (long[]) v;
            if (p + 16 > wireLimit) return -1;
            Unsafe.getUnsafe().putLong(p, pair[0]);
            Unsafe.getUnsafe().putLong(p + 8, pair[1]);
            p += 16;
        }
        return p;
    }

    private long emitLongQuad(ObjList<Object> values, long p, long wireLimit) {
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            long[] quad = (long[]) v;
            if (p + 32 > wireLimit) return -1;
            Unsafe.getUnsafe().putLong(p, quad[0]);
            Unsafe.getUnsafe().putLong(p + 8, quad[1]);
            Unsafe.getUnsafe().putLong(p + 16, quad[2]);
            Unsafe.getUnsafe().putLong(p + 24, quad[3]);
            p += 32;
        }
        return p;
    }

    /**
     * STRING/VARCHAR encoding: (N+1) x uint32 offset array, then concatenated UTF-8 bytes,
     * where N = non-null value count.
     */
    private long emitStringOrVarchar(ObjList<Object> values, byte wireType, long p, long wireLimit) {
        int nonNull = 0;
        for (int i = 0; i < rowCount; i++) if (values.getQuick(i) != NULL) nonNull++;
        // Size upper bound check
        long offsetArraySize = 4L * (nonNull + 1);
        if (p + offsetArraySize > wireLimit) return -1;

        long offsetsStart = p;
        long bytesStart = p + offsetArraySize;
        long bytesPos = bytesStart;
        int nonNullIdx = 0;
        Unsafe.getUnsafe().putInt(offsetsStart, 0);
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            byte[] raw;
            if (wireType == QwpConstants.TYPE_STRING) {
                raw = ((String) v).getBytes(StandardCharsets.UTF_8);
            } else {
                raw = (byte[]) v;
            }
            if (bytesPos + raw.length > wireLimit) return -1;
            for (byte b : raw) Unsafe.getUnsafe().putByte(bytesPos++, b);
            nonNullIdx++;
            Unsafe.getUnsafe().putInt(offsetsStart + 4L * nonNullIdx, (int) (bytesPos - bytesStart));
        }
        return bytesPos;
    }

    /**
     * SYMBOL per-table dict mode: [dict_size varint] [for each entry: entry_len varint + UTF-8]
     * [for each non-null row: dict_index varint].
     */
    private long emitSymbol(int ci, long p, long wireLimit) {
        ObjList<String> order = symbolDictOrder.getQuick(ci);
        ObjList<Object> values = columnValues.getQuick(ci);
        int dictSize = order.size();
        if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
        p = QwpVarint.encode(p, dictSize);
        for (int e = 0; e < dictSize; e++) {
            byte[] raw = order.getQuick(e).getBytes(StandardCharsets.UTF_8);
            if (p + QwpVarint.MAX_VARINT_BYTES + raw.length > wireLimit) return -1;
            p = QwpVarint.encode(p, raw.length);
            for (byte b : raw) Unsafe.getUnsafe().putByte(p++, b);
        }
        for (int i = 0; i < rowCount; i++) {
            Object v = values.getQuick(i);
            if (v == NULL) continue;
            long id = (Long) v;
            if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpVarint.encode(p, id);
        }
        return p;
    }

    private long asLongBits(Object v, int sizeBytes) {
        if (v instanceof Long l) return l;
        if (v instanceof Boolean b) return b ? 1L : 0L;
        if (v instanceof Double d) return Double.doubleToRawLongBits(d);
        if (v instanceof Float f) return Float.floatToRawIntBits(f) & 0xFFFFFFFFL;
        throw new IllegalStateException("unexpected boxed type " + v.getClass());
    }

}
