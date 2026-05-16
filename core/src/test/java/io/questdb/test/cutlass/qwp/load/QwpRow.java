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

package io.questdb.test.cutlass.qwp.load;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.Long256;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;

import java.time.temporal.ChronoUnit;

/**
 * One row of expected data for the QWP oracle. Holds typed values keyed by
 * column name. Carries the dedup keys ({@code id}, {@code tsMicros}) explicitly
 * because they govern publish order and assertion lookup.
 *
 * <p>Concurrency: a row is owned by a single producer thread until it is
 * handed off to {@link QwpTable#addRow(QwpRow)}. After hand-off it is treated
 * as immutable and may be read by any thread.
 *
 * <p>Ordering contract for {@link #publishTo}: QuestDB's {@link Sender}
 * requires {@code symbol(...)} calls before any other column setter on a row.
 * The publish loop emits symbols first, then the {@code id} column, then all
 * remaining columns in insertion order, then finalizes with {@code at(...)}.
 */
public class QwpRow {

    private final long id;
    private final ObjList<CharSequence> orderedNames = new ObjList<>();
    private final long tsMicros;
    private final LowerCaseCharSequenceObjHashMap<TypedValue> values = new LowerCaseCharSequenceObjHashMap<>();

    public QwpRow(long id, long tsMicros) {
        this.id = id;
        this.tsMicros = tsMicros;
    }

    /**
     * Walk the table's columns and assert each cell matches the oracle.
     * For a column the row never set, asserts the cursor's NULL sentinel for
     * that type.
     */
    public void assertAgainst(RecordMetadata metadata, Record record, String idColumnName, String tsColumnName, long rowOrdinal) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            String name = metadata.getColumnName(i);
            int colType = metadata.getColumnType(i);
            if (name.equals(tsColumnName)) {
                long actual = record.getTimestamp(i);
                Assert.assertEquals("ts mismatch at row " + rowOrdinal + " id=" + id, tsMicros, actual);
                continue;
            }
            if (name.equals(idColumnName)) {
                long actual = record.getLong(i);
                Assert.assertEquals("id mismatch at row " + rowOrdinal, id, actual);
                continue;
            }
            TypedValue tv = values.get(name);
            assertCell(name, colType, tv, record, i, rowOrdinal);
        }
    }

    public TypedValue get(CharSequence name) {
        return values.get(name);
    }

    public long getId() {
        return id;
    }

    public long getTimestampMicros() {
        return tsMicros;
    }

    /**
     * Publish this row through the QWP {@link Sender}. Symbols are written
     * first per the Sender contract, then the dedup {@code id}, then all
     * remaining columns in the order they were set on this row, finalized
     * with {@code at(tsMicros, MICROS)}.
     */
    public void publishTo(Sender sender, String tableName, String idColumnName) {
        // QWP WebSocket connection ("ws::") always returns a QwpWebSocketSender, which
        // exposes per-wire-type setters (byte/short/int/float/char/uuid/long256) that
        // the public Sender interface does not. We need the cast to fuzz those wire
        // types end-to-end.
        QwpWebSocketSender qwp = (QwpWebSocketSender) sender;
        qwp.table(tableName);
        for (int i = 0, n = orderedNames.size(); i < n; i++) {
            CharSequence name = orderedNames.getQuick(i);
            TypedValue v = values.get(name);
            if (v.type == ValueType.SYMBOL) {
                qwp.symbol(name, v.s);
            }
        }
        qwp.longColumn(idColumnName, id);
        for (int i = 0, n = orderedNames.size(); i < n; i++) {
            CharSequence name = orderedNames.getQuick(i);
            TypedValue v = values.get(name);
            switch (v.type) {
                case BOOLEAN:
                    qwp.boolColumn(name, v.b);
                    break;
                case BYTE:
                    qwp.byteColumn(name, v.b8);
                    break;
                case SHORT:
                    qwp.shortColumn(name, v.s16);
                    break;
                case INT:
                    qwp.intColumn(name, v.i32);
                    break;
                case LONG:
                    qwp.longColumn(name, v.l);
                    break;
                case FLOAT:
                    qwp.floatColumn(name, v.f);
                    break;
                case DOUBLE:
                    qwp.doubleColumn(name, v.d);
                    break;
                case CHAR:
                    qwp.charColumn(name, v.c);
                    break;
                case STRING:
                    qwp.stringColumn(name, v.s);
                    break;
                case UUID:
                    // QwpWebSocketSender.uuidColumn signature is (name, lo, hi).
                    qwp.uuidColumn(name, v.uuidLo, v.uuidHi);
                    break;
                case LONG256:
                    qwp.long256Column(name, v.l256_0, v.l256_1, v.l256_2, v.l256_3);
                    break;
                case TIMESTAMP_NANO:
                    qwp.timestampColumn(name, v.tsNanos, ChronoUnit.NANOS);
                    break;
                case DOUBLE_ARRAY_1D:
                    qwp.doubleArray(name, v.da1);
                    break;
                case DOUBLE_ARRAY_2D:
                    qwp.doubleArray(name, v.da2);
                    break;
                case DOUBLE_ARRAY_3D:
                    qwp.doubleArray(name, v.da3);
                    break;
                case DECIMAL64:
                    qwp.decimalColumn(name, Decimal64.fromLong(v.dec64Value, v.decScale));
                    break;
                case DECIMAL128:
                    qwp.decimalColumn(name, new Decimal128(v.dec128Hi, v.dec128Lo, v.decScale));
                    break;
                case DECIMAL256:
                    qwp.decimalColumn(name, new Decimal256(v.dec256Hh, v.dec256Hl, v.dec256Lh, v.dec256Ll, v.decScale));
                    break;
                case IPV4:
                    sender.ipv4Column(name, v.ipv4);
                    break;
                case GEOHASH:
                    // QWP wire form carries the bits + precision pair; the server
                    // auto-creates GEOHASH(Nb) sized to fit the declared precision.
                    qwp.geoHashColumn(name, v.geoHash, v.geoHashBits);
                    break;
                case BINARY:
                    sender.binaryColumn(name, v.bin);
                    break;
                case SYMBOL:
                    // already emitted
                    break;
            }
        }
        qwp.at(tsMicros, ChronoUnit.MICROS);
    }

    /**
     * Records a BINARY value as a byte slice. Pass a non-null array (use
     * {@code new byte[0]} for empty); omit the column entirely to model an
     * explicit NULL through the bitmap path.
     */
    public QwpRow setBinary(String name, byte[] value) {
        TypedValue v = put(name);
        v.type = ValueType.BINARY;
        v.bin = value;
        return this;
    }

    public void setBool(String name, boolean value) {
        TypedValue v = put(name);
        v.type = ValueType.BOOLEAN;
        v.b = value;
    }

    public void setByte(String name, byte value) {
        TypedValue v = put(name);
        v.type = ValueType.BYTE;
        v.b8 = value;
    }

    public void setChar(String name, char value) {
        TypedValue v = put(name);
        v.type = ValueType.CHAR;
        v.c = value;
    }

    public void setDecimal128(String name, long hi, long lo, int scale) {
        TypedValue v = put(name);
        v.type = ValueType.DECIMAL128;
        v.dec128Hi = hi;
        v.dec128Lo = lo;
        v.decScale = scale;
    }

    public void setDecimal256(String name, long hh, long hl, long lh, long ll, int scale) {
        TypedValue v = put(name);
        v.type = ValueType.DECIMAL256;
        v.dec256Hh = hh;
        v.dec256Hl = hl;
        v.dec256Lh = lh;
        v.dec256Ll = ll;
        v.decScale = scale;
    }

    public void setDecimal64(String name, long unscaledValue, int scale) {
        TypedValue v = put(name);
        v.type = ValueType.DECIMAL64;
        v.dec64Value = unscaledValue;
        v.decScale = scale;
    }

    /**
     * Records a packed IPv4 address. The bit pattern 0 is reserved as QuestDB's
     * IPv4 NULL sentinel; pass a non-zero address (or skip the column to model
     * an explicit NULL) to keep the oracle's expectations unambiguous.
     */
    public QwpRow setIPv4(String name, int address) {
        TypedValue v = put(name);
        v.type = ValueType.IPV4;
        v.ipv4 = address;
        return this;
    }

    public QwpRow setDouble(String name, double value) {
        TypedValue v = put(name);
        v.type = ValueType.DOUBLE;
        v.d = value;
        return this;
    }

    public void setDoubleArray1d(String name, double[] value) {
        TypedValue v = put(name);
        v.type = ValueType.DOUBLE_ARRAY_1D;
        v.da1 = value;
    }

    public void setDoubleArray2d(String name, double[][] value) {
        TypedValue v = put(name);
        v.type = ValueType.DOUBLE_ARRAY_2D;
        v.da2 = value;
    }

    public void setDoubleArray3d(String name, double[][][] value) {
        TypedValue v = put(name);
        v.type = ValueType.DOUBLE_ARRAY_3D;
        v.da3 = value;
    }

    public void setFloat(String name, float value) {
        TypedValue v = put(name);
        v.type = ValueType.FLOAT;
        v.f = value;
    }

    /**
     * Records a GEOHASH value with explicit bit precision. {@code precisionBits}
     * must be in {@code [1, 60]} and is locked at the column on the first row
     * (the server auto-creates {@code GEOHASH(Nb)} sized to fit).
     */
    public QwpRow setGeoHash(String name, long bits, int precisionBits) {
        TypedValue v = put(name);
        v.type = ValueType.GEOHASH;
        // Mask high bits so the stored expectation matches what the wire encoder
        // and server-side null bitmap interpret (low precisionBits significant).
        v.geoHash = precisionBits >= 64 ? bits : bits & ((1L << precisionBits) - 1L);
        v.geoHashBits = precisionBits;
        return this;
    }

    /**
     * Records a GEOHASH from a base32 string (one char = 5 bits). Convenience
     * for tests that want to read back exactly the string they wrote: the
     * server stores the bits and renders them back as the same base32 string.
     */
    public QwpRow setGeoHash(String name, String base32) {
        long bits = 0;
        int len = base32.length();
        for (int i = 0; i < len; i++) {
            byte b = GeoHashes.encodeChar(base32.charAt(i));
            if (b < 0) {
                throw new IllegalArgumentException("invalid GEOHASH character '" + base32.charAt(i) + "' in: " + base32);
            }
            bits = (bits << 5) | b;
        }
        return setGeoHash(name, bits, len * 5);
    }

    public void setInt(String name, int value) {
        TypedValue v = put(name);
        v.type = ValueType.INT;
        v.i32 = value;
    }

    public QwpRow setLong(String name, long value) {
        TypedValue v = put(name);
        v.type = ValueType.LONG;
        v.l = value;
        return this;
    }

    public void setLong256(String name, long l0, long l1, long l2, long l3) {
        TypedValue v = put(name);
        v.type = ValueType.LONG256;
        v.l256_0 = l0;
        v.l256_1 = l1;
        v.l256_2 = l2;
        v.l256_3 = l3;
    }

    public void setShort(String name, short value) {
        TypedValue v = put(name);
        v.type = ValueType.SHORT;
        v.s16 = value;
    }

    public QwpRow setString(String name, String value) {
        TypedValue v = put(name);
        v.type = ValueType.STRING;
        v.s = value;
        return this;
    }

    public void setSymbol(String name, String value) {
        TypedValue v = put(name);
        v.type = ValueType.SYMBOL;
        v.s = value;
    }

    public void setTimestampNano(String name, long valueNanos) {
        TypedValue v = put(name);
        v.type = ValueType.TIMESTAMP_NANO;
        v.tsNanos = valueNanos;
    }

    public void setUuid(String name, long hi, long lo) {
        TypedValue v = put(name);
        v.type = ValueType.UUID;
        v.uuidHi = hi;
        v.uuidLo = lo;
    }

    private static void assertArray1dDoubleEquals(String name, double[] expected, ArrayView actual, long rowOrdinal) {
        Assert.assertEquals("array dim count for " + name + " row=" + rowOrdinal, 1, actual.getDimCount());
        Assert.assertEquals("array length for " + name + " row=" + rowOrdinal, expected.length, actual.getDimLen(0));
        int stride = actual.getStride(0);
        for (int i = 0; i < expected.length; i++) {
            double a = actual.getDouble(i * stride);
            // tolerate NaN equality
            if (Double.isNaN(expected[i])) {
                Assert.assertTrue("expected NaN at " + name + "[" + i + "] row=" + rowOrdinal, Double.isNaN(a));
            } else {
                Assert.assertEquals(name + "[" + i + "] row=" + rowOrdinal, expected[i], a, 0.0);
            }
        }
    }

    private static void assertArray3dDoubleEquals(String name, double[][][] expected, ArrayView actual, long rowOrdinal) {
        Assert.assertEquals("array dim count for " + name + " row=" + rowOrdinal, 3, actual.getDimCount());
        int d0 = expected.length;
        int d1 = d0 == 0 ? 0 : expected[0].length;
        int d2 = d1 == 0 ? 0 : expected[0][0].length;
        Assert.assertEquals("array dim 0 for " + name + " row=" + rowOrdinal, d0, actual.getDimLen(0));
        Assert.assertEquals("array dim 1 for " + name + " row=" + rowOrdinal, d1, actual.getDimLen(1));
        Assert.assertEquals("array dim 2 for " + name + " row=" + rowOrdinal, d2, actual.getDimLen(2));
        int s0 = actual.getStride(0);
        int s1 = actual.getStride(1);
        int s2 = actual.getStride(2);
        for (int i = 0; i < d0; i++) {
            Assert.assertEquals("ragged 3d array dim 1 at i=" + i + " for " + name + " row=" + rowOrdinal, d1, expected[i].length);
            for (int j = 0; j < d1; j++) {
                Assert.assertEquals("ragged 3d array dim 2 at i=" + i + " j=" + j + " for " + name + " row=" + rowOrdinal, d2, expected[i][j].length);
                for (int k = 0; k < d2; k++) {
                    double a = actual.getDouble(i * s0 + j * s1 + k * s2);
                    if (Double.isNaN(expected[i][j][k])) {
                        Assert.assertTrue("expected NaN at " + name + "[" + i + "][" + j + "][" + k + "] row=" + rowOrdinal, Double.isNaN(a));
                    } else {
                        Assert.assertEquals(name + "[" + i + "][" + j + "][" + k + "] row=" + rowOrdinal, expected[i][j][k], a, 0.0);
                    }
                }
            }
        }
    }

    private static void assertArray2dDoubleEquals(String name, double[][] expected, ArrayView actual, long rowOrdinal) {
        Assert.assertEquals("array dim count for " + name + " row=" + rowOrdinal, 2, actual.getDimCount());
        Assert.assertEquals("array dim 0 for " + name + " row=" + rowOrdinal, expected.length, actual.getDimLen(0));
        int rows = expected.length;
        int cols = rows == 0 ? 0 : expected[0].length;
        Assert.assertEquals("array dim 1 for " + name + " row=" + rowOrdinal, cols, actual.getDimLen(1));
        int s0 = actual.getStride(0);
        int s1 = actual.getStride(1);
        for (int i = 0; i < rows; i++) {
            Assert.assertEquals("ragged 2d array row " + i + " for " + name + " row=" + rowOrdinal, cols, expected[i].length);
            for (int j = 0; j < cols; j++) {
                double a = actual.getDouble(i * s0 + j * s1);
                if (Double.isNaN(expected[i][j])) {
                    Assert.assertTrue("expected NaN at " + name + "[" + i + "][" + j + "] row=" + rowOrdinal, Double.isNaN(a));
                } else {
                    Assert.assertEquals(name + "[" + i + "][" + j + "] row=" + rowOrdinal, expected[i][j], a, 0.0);
                }
            }
        }
    }

    private static void assertCell(String name, int colType, TypedValue tv, Record record, int columnIndex, long rowOrdinal) {
        // TIMESTAMP_NANO and TIMESTAMP share the same tag (TIMESTAMP); the high bit
        // discriminates them. Handle TIMESTAMP_NANO before falling into the tag switch.
        if (ColumnType.isTimestampNano(colType)) {
            long actual = record.getTimestamp(columnIndex);
            if (tv == null) {
                Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual);
            } else {
                Assert.assertEquals(name + " row=" + rowOrdinal, tv.tsNanos, actual);
            }
            return;
        }
        short tag = ColumnType.tagOf(colType);
        switch (tag) {
            case ColumnType.BOOLEAN: {
                // BOOLEAN has no NULL; absent column reads as false. Producers
                // for the oracle MUST always set BOOLEAN columns.
                boolean expected = tv != null && tv.b;
                Assert.assertEquals(name + " row=" + rowOrdinal, expected, record.getBool(columnIndex));
                break;
            }
            case ColumnType.BYTE: {
                // BYTE has no NULL; absent column reads as 0. Producers must always set it.
                byte expected = tv == null ? 0 : tv.b8;
                Assert.assertEquals(name + " row=" + rowOrdinal, expected, record.getByte(columnIndex));
                break;
            }
            case ColumnType.SHORT: {
                // SHORT has no NULL; absent column reads as 0. Producers must always set it.
                short expected = tv == null ? 0 : tv.s16;
                Assert.assertEquals(name + " row=" + rowOrdinal, expected, record.getShort(columnIndex));
                break;
            }
            case ColumnType.INT: {
                int actual = record.getInt(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, Numbers.INT_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.i32, actual);
                }
                break;
            }
            case ColumnType.LONG: {
                long actual = record.getLong(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.l, actual);
                }
                break;
            }
            case ColumnType.IPv4: {
                int actual = record.getIPv4(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, Numbers.IPv4_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.ipv4, actual);
                }
                break;
            }
            case ColumnType.GEOBYTE: {
                // Server auto-creates GEOHASH(<=8b) as GEOBYTE storage; tv.geoHash
                // already holds the low precisionBits, sign-narrow to byte to match.
                byte actual = record.getGeoByte(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, GeoHashes.BYTE_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, (byte) tv.geoHash, actual);
                }
                break;
            }
            case ColumnType.GEOSHORT: {
                short actual = record.getGeoShort(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, GeoHashes.SHORT_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, (short) tv.geoHash, actual);
                }
                break;
            }
            case ColumnType.GEOINT: {
                int actual = record.getGeoInt(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, GeoHashes.INT_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, (int) tv.geoHash, actual);
                }
                break;
            }
            case ColumnType.GEOLONG: {
                long actual = record.getGeoLong(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, GeoHashes.NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.geoHash, actual);
                }
                break;
            }
            case ColumnType.FLOAT: {
                float actual = record.getFloat(columnIndex);
                if (tv == null) {
                    Assert.assertTrue(name + " expected NULL row=" + rowOrdinal, Float.isNaN(actual));
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.f, actual, 0.0f);
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double actual = record.getDouble(columnIndex);
                if (tv == null) {
                    Assert.assertFalse(name + " expected NULL row=" + rowOrdinal, Numbers.isFinite(actual));
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.d, actual, 0.0);
                }
                break;
            }
            case ColumnType.CHAR: {
                // CHAR NULL is '\0'; absent column reads as '\0'. Producers must always set it.
                char expected = tv == null ? Numbers.CHAR_NULL : tv.c;
                Assert.assertEquals(name + " row=" + rowOrdinal, expected, record.getChar(columnIndex));
                break;
            }
            case ColumnType.UUID: {
                long actualHi = record.getLong128Hi(columnIndex);
                long actualLo = record.getLong128Lo(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + ".hi expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actualHi);
                    Assert.assertEquals(name + ".lo expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actualLo);
                } else {
                    Assert.assertEquals(name + ".hi row=" + rowOrdinal, tv.uuidHi, actualHi);
                    Assert.assertEquals(name + ".lo row=" + rowOrdinal, tv.uuidLo, actualLo);
                }
                break;
            }
            case ColumnType.LONG256: {
                Long256 actual = record.getLong256A(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + ".l0 expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual.getLong0());
                    Assert.assertEquals(name + ".l1 expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual.getLong1());
                    Assert.assertEquals(name + ".l2 expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual.getLong2());
                    Assert.assertEquals(name + ".l3 expected NULL row=" + rowOrdinal, Numbers.LONG_NULL, actual.getLong3());
                } else {
                    Assert.assertEquals(name + ".l0 row=" + rowOrdinal, tv.l256_0, actual.getLong0());
                    Assert.assertEquals(name + ".l1 row=" + rowOrdinal, tv.l256_1, actual.getLong1());
                    Assert.assertEquals(name + ".l2 row=" + rowOrdinal, tv.l256_2, actual.getLong2());
                    Assert.assertEquals(name + ".l3 row=" + rowOrdinal, tv.l256_3, actual.getLong3());
                }
                break;
            }
            case ColumnType.STRING: {
                CharSequence actual = record.getStrA(columnIndex);
                if (tv == null) {
                    Assert.assertNull(name + " expected NULL row=" + rowOrdinal, actual);
                } else {
                    Assert.assertNotNull(name + " unexpectedly NULL row=" + rowOrdinal, actual);
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.s, actual.toString());
                }
                break;
            }
            case ColumnType.VARCHAR: {
                // QWP server auto-creates string columns as VARCHAR. The
                // oracle still tracks them as STRING semantically.
                Utf8Sequence actual = record.getVarcharA(columnIndex);
                if (tv == null) {
                    Assert.assertNull(name + " expected NULL row=" + rowOrdinal, actual);
                } else {
                    Assert.assertNotNull(name + " unexpectedly NULL row=" + rowOrdinal, actual);
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.s, actual.toString());
                }
                break;
            }
            case ColumnType.BINARY: {
                io.questdb.std.BinarySequence actual = record.getBin(columnIndex);
                if (tv == null) {
                    Assert.assertNull(name + " expected NULL row=" + rowOrdinal, actual);
                } else {
                    Assert.assertNotNull(name + " unexpectedly NULL row=" + rowOrdinal, actual);
                    Assert.assertEquals(name + " length row=" + rowOrdinal, tv.bin.length, actual.length());
                    for (int i = 0; i < tv.bin.length; i++) {
                        Assert.assertEquals(
                                name + " byte[" + i + "] row=" + rowOrdinal,
                                tv.bin[i],
                                actual.byteAt(i)
                        );
                    }
                }
                break;
            }
            case ColumnType.SYMBOL: {
                CharSequence actual = record.getSymA(columnIndex);
                if (tv == null) {
                    Assert.assertNull(name + " expected NULL row=" + rowOrdinal, actual);
                } else {
                    Assert.assertNotNull(name + " unexpectedly NULL row=" + rowOrdinal, actual);
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.s, actual.toString());
                }
                break;
            }
            case ColumnType.DECIMAL64: {
                long actual = record.getDecimal64(columnIndex);
                if (tv == null) {
                    Assert.assertEquals(name + " expected NULL row=" + rowOrdinal, Decimals.DECIMAL64_NULL, actual);
                } else {
                    Assert.assertEquals(name + " row=" + rowOrdinal, tv.dec64Value, actual);
                }
                break;
            }
            case ColumnType.DECIMAL128: {
                io.questdb.std.Decimal128 sink = new io.questdb.std.Decimal128();
                record.getDecimal128(columnIndex, sink);
                if (tv == null) {
                    Assert.assertTrue(name + " expected NULL row=" + rowOrdinal, sink.isNull());
                } else {
                    Assert.assertEquals(name + ".hi row=" + rowOrdinal, tv.dec128Hi, sink.getHigh());
                    Assert.assertEquals(name + ".lo row=" + rowOrdinal, tv.dec128Lo, sink.getLow());
                }
                break;
            }
            case ColumnType.DECIMAL256: {
                io.questdb.std.Decimal256 sink = new io.questdb.std.Decimal256();
                record.getDecimal256(columnIndex, sink);
                if (tv == null) {
                    Assert.assertTrue(name + " expected NULL row=" + rowOrdinal, sink.isNull());
                } else {
                    Assert.assertEquals(name + ".hh row=" + rowOrdinal, tv.dec256Hh, sink.getHh());
                    Assert.assertEquals(name + ".hl row=" + rowOrdinal, tv.dec256Hl, sink.getHl());
                    Assert.assertEquals(name + ".lh row=" + rowOrdinal, tv.dec256Lh, sink.getLh());
                    Assert.assertEquals(name + ".ll row=" + rowOrdinal, tv.dec256Ll, sink.getLl());
                }
                break;
            }
            case ColumnType.ARRAY: {
                ArrayView arr = record.getArray(columnIndex, colType);
                if (tv == null) {
                    Assert.assertTrue(name + " expected NULL array row=" + rowOrdinal, arr.isNull());
                    break;
                }
                switch (tv.type) {
                    case DOUBLE_ARRAY_1D:
                        assertArray1dDoubleEquals(name, tv.da1, arr, rowOrdinal);
                        break;
                    case DOUBLE_ARRAY_2D:
                        assertArray2dDoubleEquals(name, tv.da2, arr, rowOrdinal);
                        break;
                    case DOUBLE_ARRAY_3D:
                        assertArray3dDoubleEquals(name, tv.da3, arr, rowOrdinal);
                        break;
                    default:
                        Assert.fail("oracle has non-array TypedValue for ARRAY column " + name + " row=" + rowOrdinal);
                }
                break;
            }
            default:
                Assert.fail("unsupported column type for " + name + ": " + ColumnType.nameOf(colType));
        }
    }

    private TypedValue put(String name) {
        TypedValue existing = values.get(name);
        if (existing != null) {
            return existing;
        }
        TypedValue v = new TypedValue();
        values.put(name, v);
        orderedNames.add(name);
        return v;
    }

    public enum ValueType {
        BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAR, STRING, SYMBOL,
        UUID, LONG256, TIMESTAMP_NANO, IPV4, GEOHASH, BINARY,
        DOUBLE_ARRAY_1D, DOUBLE_ARRAY_2D, DOUBLE_ARRAY_3D,
        DECIMAL64, DECIMAL128, DECIMAL256
    }

    public static final class TypedValue {
        public boolean b;
        public byte b8;
        public byte[] bin;
        public char c;
        public double d;
        public double[] da1;
        public double[][] da2;
        public double[][][] da3;
        public long dec128Hi;
        public long dec128Lo;
        public long dec256Hh;
        public long dec256Hl;
        public long dec256Lh;
        public long dec256Ll;
        public long dec64Value;
        public int decScale;
        public float f;
        public long geoHash;
        public int geoHashBits;
        public int i32;
        public int ipv4;
        public long l;
        public long l256_0;
        public long l256_1;
        public long l256_2;
        public long l256_3;
        public String s;
        public short s16;
        public long tsNanos;
        public ValueType type;
        public long uuidHi;
        public long uuidLo;
    }
}
