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
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

/**
 * Column-major accumulator for one QWP egress {@code RESULT_BATCH} table block.
 * <p>
 * Each column has its own {@link QwpColumnScratch} with type-specialised native
 * buffers. {@code appendRow} writes directly to native memory (no JVM-heap
 * allocations on the hot path after warmup). {@code emitTableBlock} memcpys
 * the scratch into the caller-owned wire buffer in one pass per column.
 * <p>
 * One instance is reused across batches on the same connection; the scratch
 * buffers grow to the maximum size observed and never shrink.
 */
public class QwpResultBatchBuffer implements QuietCloseable {

    private final ObjList<QwpColumnScratch> scratches = new ObjList<>();
    private ObjList<QwpEgressColumnDef> columns;
    private int columnCount;
    private int rowCount;

    public QwpResultBatchBuffer() {
    }

    /**
     * Appends one row's worth of values from the given record.
     */
    public void appendRow(Record record) {
        for (int ci = 0; ci < columnCount; ci++) {
            QwpEgressColumnDef def = columns.getQuick(ci);
            QwpColumnScratch scratch = scratches.getQuick(ci);
            int qdbType = def.getQuestdbColumnType();
            byte wire = def.getWireType();
            switch (wire) {
                case QwpConstants.TYPE_BOOLEAN:
                    scratch.appendBool(record.getBool(ci));
                    break;
                case QwpConstants.TYPE_BYTE:
                    scratch.appendByte(record.getByte(ci));
                    break;
                case QwpConstants.TYPE_SHORT:
                    scratch.appendShort(record.getShort(ci));
                    break;
                case QwpConstants.TYPE_CHAR:
                    scratch.appendChar(record.getChar(ci));
                    break;
                case QwpConstants.TYPE_INT: {
                    int v = record.getInt(ci);
                    if (v == Numbers.INT_NULL) scratch.appendNull();
                    else scratch.appendInt(v);
                    break;
                }
                case QwpConstants.TYPE_LONG: {
                    long v = record.getLong(ci);
                    if (v == Numbers.LONG_NULL) scratch.appendNull();
                    else scratch.appendLong(v);
                    break;
                }
                case QwpConstants.TYPE_DATE: {
                    long v = record.getDate(ci);
                    if (v == Numbers.LONG_NULL) scratch.appendNull();
                    else scratch.appendLong(v);
                    break;
                }
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS: {
                    long v = record.getTimestamp(ci);
                    if (v == Numbers.LONG_NULL) scratch.appendNull();
                    else scratch.appendLong(v);
                    break;
                }
                case QwpConstants.TYPE_FLOAT: {
                    float v = record.getFloat(ci);
                    if (Float.isNaN(v)) scratch.appendNull();
                    else scratch.appendFloat(v);
                    break;
                }
                case QwpConstants.TYPE_DOUBLE: {
                    double v = record.getDouble(ci);
                    if (Double.isNaN(v)) scratch.appendNull();
                    else scratch.appendDouble(v);
                    break;
                }
                case QwpConstants.TYPE_STRING: {
                    CharSequence cs = record.getStrA(ci);
                    if (cs == null) scratch.appendNull();
                    else scratch.appendString(cs);
                    break;
                }
                case QwpConstants.TYPE_VARCHAR: {
                    Utf8Sequence us = record.getVarcharA(ci);
                    if (us == null) scratch.appendNull();
                    else scratch.appendVarchar(us);
                    break;
                }
                case QwpConstants.TYPE_SYMBOL: {
                    CharSequence cs = record.getSymA(ci);
                    if (cs == null) scratch.appendNull();
                    else scratch.appendSymbol(cs);
                    break;
                }
                case QwpConstants.TYPE_UUID: {
                    long lo = record.getLong128Lo(ci);
                    long hi = record.getLong128Hi(ci);
                    if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) scratch.appendNull();
                    else scratch.appendUuid(lo, hi);
                    break;
                }
                case QwpConstants.TYPE_LONG256: {
                    Long256 l256 = record.getLong256A(ci);
                    if (l256 == null || (l256.getLong0() == Numbers.LONG_NULL
                            && l256.getLong1() == Numbers.LONG_NULL
                            && l256.getLong2() == Numbers.LONG_NULL
                            && l256.getLong3() == Numbers.LONG_NULL)) {
                        scratch.appendNull();
                    } else {
                        scratch.appendLong256(l256.getLong0(), l256.getLong1(), l256.getLong2(), l256.getLong3());
                    }
                    break;
                }
                case QwpConstants.TYPE_GEOHASH: {
                    int precBits = def.getPrecisionBits();
                    long bits = readGeoBits(record, ci, precBits);
                    if (bits == -1L) scratch.appendNull();
                    else scratch.appendGeohash(bits, (precBits + 7) >>> 3);
                    break;
                }
                case QwpConstants.TYPE_DECIMAL64: {
                    long v = record.getDecimal64(ci);
                    if (v == Numbers.LONG_NULL) scratch.appendNull();
                    else scratch.appendLong(v);
                    break;
                }
                case QwpConstants.TYPE_DECIMAL128: {
                    record.getDecimal128(ci, scratch.decimal128Sink);
                    if (scratch.decimal128Sink.isNull()) scratch.appendNull();
                    else scratch.appendDecimal128(scratch.decimal128Sink.getLow(), scratch.decimal128Sink.getHigh());
                    break;
                }
                case QwpConstants.TYPE_DECIMAL256: {
                    record.getDecimal256(ci, scratch.decimal256Sink);
                    if (scratch.decimal256Sink.isNull()) scratch.appendNull();
                    else scratch.appendDecimal256(
                            scratch.decimal256Sink.getLl(),
                            scratch.decimal256Sink.getLh(),
                            scratch.decimal256Sink.getHl(),
                            scratch.decimal256Sink.getHh());
                    break;
                }
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                case QwpConstants.TYPE_LONG_ARRAY: {
                    ArrayView av = record.getArray(ci, qdbType);
                    if (av == null || av.isNull()) {
                        scratch.appendNull();
                    } else {
                        appendArrayBytesDirect(scratch, av, wire);
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException(
                            "QWP egress append: unsupported wire type 0x" + Integer.toHexString(wire & 0xFF));
            }
        }
        rowCount++;
    }

    /**
     * Starts a new batch with the given schema. Re-sizes the scratch pool and resets each scratch.
     */
    public void beginBatch(ObjList<QwpEgressColumnDef> columns) {
        this.columns = columns;
        this.columnCount = columns.size();
        this.rowCount = 0;
        while (scratches.size() < columnCount) {
            scratches.add(new QwpColumnScratch());
        }
        for (int i = 0; i < columnCount; i++) {
            scratches.getQuick(i).beginBatch(columns.getQuick(i));
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).close();
        }
        scratches.clear();
        columns = null;
        rowCount = 0;
        columnCount = 0;
    }

    /**
     * Emits the full table block body for this batch starting at {@code wireBuf}:
     * name_length (=0) + row_count + column_count + schema + column data sections.
     *
     * @return number of bytes written, or -1 if the data would overflow wireLimit
     */
    public int emitTableBlock(long wireBuf, long wireLimit, long schemaId, boolean writeFullSchema) {
        long p = wireBuf;
        if (p >= wireLimit) return -1;
        // Anonymous result-set: empty name
        Unsafe.getUnsafe().putByte(p++, (byte) 0);
        p = QwpVarint.encode(p, rowCount);
        p = QwpVarint.encode(p, columnCount);
        if (writeFullSchema) {
            int needed = QwpEgressSchemaWriter.worstCaseFullSize(columns);
            if (p + needed > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeFull(p, schemaId, columns);
        } else {
            if (p + 1 + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeReference(p, schemaId);
        }
        for (int ci = 0; ci < columnCount; ci++) {
            p = emitColumn(scratches.getQuick(ci), p, wireLimit);
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
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).beginBatch(null);
        }
        rowCount = 0;
        columnCount = 0;
        columns = null;
    }

    /**
     * Serialises an array into {@code scratch.arrayHeapAddr} without allocating a {@code byte[]}.
     * Format: {@code [nDims u8] [dim lengths: nDims × i32 LE] [values: product(dims) × 8 bytes LE]}.
     */
    private static void appendArrayBytesDirect(QwpColumnScratch scratch, ArrayView av, byte wireType) {
        int nDims = av.getDimCount();
        int elements = 1;
        for (int d = 0; d < nDims; d++) elements *= av.getDimLen(d);
        int totalBytes = 1 + 4 * nDims + 8 * elements;
        scratch.ensureArrayHeapCapacity(scratch.arrayHeapPos + totalBytes);
        long p = scratch.arrayHeapAddr + scratch.arrayHeapPos;
        Unsafe.getUnsafe().putByte(p++, (byte) nDims);
        for (int d = 0; d < nDims; d++) {
            Unsafe.getUnsafe().putInt(p, av.getDimLen(d));
            p += 4;
        }
        if (wireType == QwpConstants.TYPE_DOUBLE_ARRAY) {
            for (int i = 0; i < elements; i++) {
                Unsafe.getUnsafe().putLong(p, Double.doubleToRawLongBits(av.getDouble(i)));
                p += 8;
            }
        } else {
            for (int i = 0; i < elements; i++) {
                Unsafe.getUnsafe().putLong(p, av.getLong(i));
                p += 8;
            }
        }
        scratch.arrayHeapPos += totalBytes;
        scratch.markNonNullAndAdvanceRow();
    }

    /**
     * Copies the null flag + bitmap (if any) for this column to the wire. Also
     * memcpys the dense value bytes for the simple cases (BOOLEAN, fixed-width).
     */
    private long emitColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        // 1. Null flag + optional bitmap
        if (scratch.nullCount == 0) {
            if (p >= wireLimit) return -1;
            Unsafe.getUnsafe().putByte(p++, (byte) 0);
        } else {
            int bitmapBytes = (rowCount + 7) >>> 3;
            if (p + 1 + bitmapBytes > wireLimit) return -1;
            Unsafe.getUnsafe().putByte(p++, (byte) 1);
            Unsafe.getUnsafe().copyMemory(scratch.nullBitmapAddr, p, bitmapBytes);
            p += bitmapBytes;
        }

        byte wire = scratch.def.getWireType();
        int nonNull = scratch.nonNullCount;

        if (wire == QwpConstants.TYPE_BOOLEAN) {
            int bytes = (nonNull + 7) >>> 3;
            if (p + bytes > wireLimit) return -1;
            Unsafe.getUnsafe().copyMemory(scratch.valuesAddr, p, bytes);
            return p + bytes;
        }
        if (wire == QwpConstants.TYPE_STRING || wire == QwpConstants.TYPE_VARCHAR) {
            return emitStringColumn(scratch, p, wireLimit);
        }
        if (wire == QwpConstants.TYPE_SYMBOL) {
            return emitSymbolColumn(scratch, p, wireLimit);
        }
        if (wire == QwpConstants.TYPE_GEOHASH) {
            if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpVarint.encode(p, scratch.def.getPrecisionBits());
            if (p + scratch.valuesPos > wireLimit) return -1;
            Unsafe.getUnsafe().copyMemory(scratch.valuesAddr, p, scratch.valuesPos);
            return p + scratch.valuesPos;
        }
        if (wire == QwpConstants.TYPE_DOUBLE_ARRAY || wire == QwpConstants.TYPE_LONG_ARRAY) {
            if (p + scratch.arrayHeapPos > wireLimit) return -1;
            Unsafe.getUnsafe().copyMemory(scratch.arrayHeapAddr, p, scratch.arrayHeapPos);
            return p + scratch.arrayHeapPos;
        }

        // Decimal: scale byte prefix, then dense values
        if (wire == QwpConstants.TYPE_DECIMAL64
                || wire == QwpConstants.TYPE_DECIMAL128
                || wire == QwpConstants.TYPE_DECIMAL256) {
            if (p + 1 > wireLimit) return -1;
            Unsafe.getUnsafe().putByte(p++, (byte) scratch.def.getScale());
            if (p + scratch.valuesPos > wireLimit) return -1;
            Unsafe.getUnsafe().copyMemory(scratch.valuesAddr, p, scratch.valuesPos);
            return p + scratch.valuesPos;
        }

        // Generic fixed-width (BYTE/SHORT/INT/CHAR/LONG/FLOAT/DOUBLE/DATE/TIMESTAMP/TIMESTAMP_NANOS/UUID/LONG256)
        if (p + scratch.valuesPos > wireLimit) return -1;
        Unsafe.getUnsafe().copyMemory(scratch.valuesAddr, p, scratch.valuesPos);
        return p + scratch.valuesPos;
    }

    private static long emitStringColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        int nonNull = scratch.nonNullCount;
        long offsetsBytes = 4L * (nonNull + 1);
        long bytesBytes = scratch.stringHeapPos;
        if (p + offsetsBytes + bytesBytes > wireLimit) return -1;
        // offset[0] = 0; offsets[1..nonNull] were populated during append.
        Unsafe.getUnsafe().putInt(p, 0);
        // Copy offsets[1..nonNull] from scratch.stringOffsetsAddr (which stores them at indices 1..nonNull)
        // to p + 4 .. p + 4 * nonNull + 4.
        Unsafe.getUnsafe().copyMemory(
                scratch.stringOffsetsAddr + 4L, p + 4L, 4L * nonNull);
        Unsafe.getUnsafe().copyMemory(scratch.stringHeapAddr, p + offsetsBytes, bytesBytes);
        return p + offsetsBytes + bytesBytes;
    }

    private static long emitSymbolColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        int dictSize = scratch.symbolDictOrder.size();
        if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
        p = QwpVarint.encode(p, dictSize);
        for (int e = 0; e < dictSize; e++) {
            // Entries remain on the heap (Strings in the dict); encode length + UTF-8 bytes.
            String entry = scratch.symbolDictOrder.getQuick(e);
            int entryLen = utf8Length(entry);
            if (p + QwpVarint.MAX_VARINT_BYTES + entryLen > wireLimit) return -1;
            p = QwpVarint.encode(p, entryLen);
            p = writeUtf8(entry, p);
        }
        // Per-row varint IDs: each row's dict id is in scratch.symbolIdsAddr[row] (i32).
        int nonNull = scratch.nonNullCount;
        for (int i = 0; i < nonNull; i++) {
            if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            int id = Unsafe.getUnsafe().getInt(scratch.symbolIdsAddr + 4L * i);
            p = QwpVarint.encode(p, id);
        }
        return p;
    }

    private static long readGeoBits(Record record, int col, int precisionBits) {
        if (precisionBits <= 7) return record.getGeoByte(col);
        if (precisionBits <= 15) return record.getGeoShort(col);
        if (precisionBits <= 31) return record.getGeoInt(col);
        return record.getGeoLong(col);
    }

    private static int utf8Length(String s) {
        int len = 0;
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) len++;
            else if (c < 0x800) len += 2;
            else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                len += 4;
                i++;
            } else len += 3;
        }
        return len;
    }

    private static long writeUtf8(String s, long p) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(p++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(p++, (byte) (0xC0 | (c >> 6)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, s.charAt(i + 1));
                i++;
                Unsafe.getUnsafe().putByte(p++, (byte) (0xF0 | (cp >> 18)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | (cp & 0x3F)));
            } else {
                Unsafe.getUnsafe().putByte(p++, (byte) (0xE0 | (c >> 12)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(p++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        return p;
    }
}
