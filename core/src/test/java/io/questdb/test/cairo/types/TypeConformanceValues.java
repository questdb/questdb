/*******************************************************************************
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

package io.questdb.test.cairo.types;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.FixedSizeTypeDriver;
import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TypeDriver;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * The value rows of the conformance kit (spec PA-18), per kit type.
 * <p>
 * Every type has a {@code null} row. Existing types add, where the type has them: {@code min}
 * and {@code max}; {@code sentinel}, the type's NULL bit pattern written as a value, which
 * reads as NULL; for types without NULL (BOOLEAN excepted, whose every bit pattern is false or
 * true) {@code other_null}, a value equal to another type's sentinel (-1, the geohash NULL),
 * which reads as data; for FLOAT and DOUBLE {@code nan} (their sentinel), {@code negzero},
 * {@code literal_inf} (SQL stores every non-finite value as NaN) and the raw rows {@code inf}
 * and {@code ninf}, which store infinities; for var-size types {@code empty} and {@code escape}, a
 * value with the characters the text protocols quote. A type has no {@code sentinel} row where
 * the pattern lies outside what a literal can write: geohash (the bits are masked), decimal (the
 * pattern is outside the declared precision) and the var-size types (NULL lives in the length
 * or the aux entry).
 * <p>
 * Existing types write their rows as SQL literals, except where SQL cannot write the value
 * (infinities). A type registered later has no literal yet: apart from {@code null}, its rows
 * are raw bit patterns derived from its type definition: {@code zero}, {@code one},
 * {@code ones}, {@code sentinel} (its own {@code getNullLong}) and {@code sentinel_<TAG>}, the
 * NULL pattern of every existing type of the same width (for a full-range type the legacy
 * sentinels, the #6921 collision); the arithmetic tier of its definition adds {@code min},
 * {@code max} and the float rows. Raw rows go
 * through the table writer by width ({@link #writeRows}) and come after the literal rows.
 * <p>
 * The table shapes the kit also runs: an empty table, an empty partition (a partition the
 * query's interval selects no row from, and a day between two partitions) and a single row.
 */
public final class TypeConformanceValues {
    public static final String SHAPE_EMPTY_PARTITION = "empty_partition";
    public static final String SHAPE_EMPTY_TABLE = "empty_table";
    public static final String SHAPE_SINGLE_ROW = "single_row";
    public static final long SECOND = 1_000_000L;

    private TypeConformanceValues() {
    }

    /**
     * The value rows of a kit type, in the order the kit writes them: row {@code i} of a
     * table has timestamp {@code i} seconds.
     */
    public static ObjList<Row> rowsOf(TypeConformanceTypes.Entry type) {
        final ObjList<Row> rows = new ObjList<>();
        // rows written raw come last, so an in-order write never goes back in time
        final ObjList<Row> rawRows = new ObjList<>();
        if (type.isLater()) {
            addDerivedRows(type, rawRows);
            rows.add(new Row("null", "NULL"));
            rows.addAll(rawRows);
            return rows;
        }
        final int columnType = type.columnType;
        final String cast = "::" + type.ddl;
        switch (type.label) {
            case "BOOLEAN" -> {
                rows.add(new Row("min", "false"));
                rows.add(new Row("max", "true"));
            }
            case "BYTE" -> addIntegral(rows, "(-128)" + cast, "127" + cast, null, "(-1)" + cast);
            case "SHORT" -> addIntegral(rows, "(-32768)" + cast, "32767" + cast, null, "(-1)" + cast);
            case "CHAR" -> addIntegral(rows, "0" + cast, "65535" + cast, null, "(-1)" + cast);
            case "INT", "IPv4" -> {
                if (columnType == ColumnType.INT) {
                    addIntegral(rows, "(-2147483647)" + cast, "2147483647" + cast, "(-2147483648)" + cast, null);
                } else {
                    addIntegral(rows, "'0.0.0.1'" + cast, "'255.255.255.255'" + cast, "'0.0.0.0'" + cast, null);
                }
            }
            case "LONG", "DATE", "TIMESTAMP", "TIMESTAMP_NS" ->
                    addIntegral(rows, "(-9223372036854775807)" + cast, "9223372036854775807" + cast, "(-9223372036854775807 - 1)" + cast, null);
            case "FLOAT" -> {
                addFloat(rows, "(-3.4028234663852886E38)" + cast, "3.4028234663852886E38" + cast, cast);
                rawRows.add(Row.bits("inf", 4, Float.floatToRawIntBits(Float.POSITIVE_INFINITY), 0, 0, 0));
                rawRows.add(Row.bits("ninf", 4, Float.floatToRawIntBits(Float.NEGATIVE_INFINITY), 0, 0, 0));
            }
            case "DOUBLE" -> {
                addFloat(rows, "(-1.7976931348623157E308)" + cast, "1.7976931348623157E308" + cast, cast);
                rawRows.add(Row.bits("inf", 8, Double.doubleToRawLongBits(Double.POSITIVE_INFINITY), 0, 0, 0));
                rawRows.add(Row.bits("ninf", 8, Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY), 0, 0, 0));
            }
            case "STRING", "SYMBOL", "VARCHAR", "VARCHAR_SLICE" -> addText(rows, cast);
            case "BINARY" -> {
                rows.add(new Row("empty", "from_base64('')"));
                rows.add(new Row("max", "from_base64('AAEC/f7/')"));
            }
            case "LONG256" -> addIntegral(
                    rows,
                    "0x0000000000000000000000000000000000000000000000000000000000000000" + cast,
                    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" + cast,
                    "0x8000000000000000800000000000000080000000000000008000000000000000" + cast,
                    null
            );
            case "UUID" -> addIntegral(
                    rows,
                    "'00000000-0000-0000-0000-000000000000'" + cast,
                    "'ffffffff-ffff-ffff-ffff-ffffffffffff'" + cast,
                    "'80000000-0000-0000-8000-000000000000'" + cast,
                    null
            );
            case "LONG128" -> addIntegral(
                    rows,
                    "to_long128(0, 0)",
                    "to_long128(-1, -1)",
                    "to_long128(-9223372036854775807 - 1, -9223372036854775807 - 1)",
                    null
            );
            case "DOUBLE[]" -> addArray(rows, 1);
            case "DOUBLE[][]" -> addArray(rows, 2);
            case "INTERVAL", "INTERVAL(us)", "INTERVAL(ns)" ->
                    rows.add(new Row("max", "interval(0::TIMESTAMP, 1::TIMESTAMP)"));
            default -> {
                if (ColumnType.isGeoHash(columnType)) {
                    addGeoHash(rows, ColumnType.getGeoHashBits(columnType));
                } else if (ColumnType.isDecimal(columnType)) {
                    addDecimal(rows, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), cast);
                } else {
                    throw new IllegalStateException("no value rows for " + type.label);
                }
            }
        }
        rows.add(new Row("null", "NULL"));
        rows.addAll(rawRows);
        return rows;
    }

    /**
     * Writes value rows {@code lo..hi} (every {@code step}-th) of a kit type into a table with
     * columns {@code k VARCHAR}, {@code v} and designated timestamp {@code ts}, row {@code i} at
     * {@code base + i} seconds, with {@code prefix} before each row label in k. Rows with a
     * literal go in one INSERT; rows with raw bits through the table writer API, by width,
     * after them. With {@code withValue} false only k and ts are written. Errors are appended to
     * {@code errors}, one line each; a row the writer refuses is cancelled.
     */
    public static void writeRows(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String table,
            ObjList<Row> rows,
            String prefix,
            long base,
            int lo,
            int hi,
            int step,
            boolean withValue,
            StringSink errors
    ) {
        final StringSink values = new StringSink();
        boolean hasRaw = false;
        for (int i = lo; i < hi; i += step) {
            final Row row = rows.getQuick(i);
            if (withValue && row.literal == null) {
                hasRaw = true;
                continue;
            }
            if (values.length() > 0) {
                values.put(", ");
            }
            values.put("('").put(prefix).put(row.label).put("', ");
            if (withValue) {
                values.put(row.literal).put(", ");
            }
            values.put(base + i * SECOND).put("::TIMESTAMP)");
        }
        if (values.length() > 0) {
            try {
                engine.execute("INSERT INTO " + table + (withValue ? " (k, v, ts)" : " (k, ts)") + " VALUES " + values, executionContext);
            } catch (Throwable e) {
                errors.put("error: insert ").put(prefix).put(": ").put(e.getMessage()).put('\n');
            }
        }
        if (!hasRaw) {
            return;
        }
        try (TableWriterAPI writer = engine.getTableWriterAPI(table, "type conformance kit")) {
            final int kIndex = writer.getMetadata().getColumnIndex("k");
            final int vIndex = writer.getMetadata().getColumnIndex("v");
            for (int i = lo; i < hi; i += step) {
                final Row value = rows.getQuick(i);
                if (value.literal != null) {
                    continue;
                }
                final TableWriter.Row row = writer.newRow(base + i * SECOND);
                try {
                    row.putVarchar(kIndex, new Utf8String(prefix + value.label));
                    writeBits(row, vIndex, value);
                    row.append();
                } catch (Throwable e) {
                    row.cancel();
                    errors.put("error: write ").put(prefix).put(value.label).put(": ").put(e.getMessage()).put('\n');
                }
            }
            writer.commit();
        } catch (Throwable e) {
            errors.put("error: write ").put(prefix).put(": ").put(e.getMessage()).put('\n');
        }
    }

    private static void writeBits(TableWriter.Row row, int column, Row value) {
        switch (value.width) {
            case 1 -> row.putByte(column, (byte) value.bits[0]);
            case 2 -> row.putShort(column, (short) value.bits[0]);
            case 4 -> row.putInt(column, (int) value.bits[0]);
            case 8 -> row.putLong(column, value.bits[0]);
            case 16 -> row.putLong128(column, value.bits[0], value.bits[1]);
            case 32 -> row.putLong256(column, value.bits[0], value.bits[1], value.bits[2], value.bits[3]);
            default -> throw new AssertionError("no raw write for width " + value.width);
        }
    }

    private static void addArray(ObjList<Row> rows, int dims) {
        final String open = dims == 1 ? "" : "[";
        final String close = dims == 1 ? "" : "]";
        final String cast = dims == 1 ? "::DOUBLE[]" : "::DOUBLE[][]";
        rows.add(new Row("min", "ARRAY[" + open + "-1.7976931348623157E308" + close + "]"));
        rows.add(new Row("max", "ARRAY[" + open + "1.7976931348623157E308" + close + "]"));
        rows.add(new Row("empty", "ARRAY[]" + cast));
        rows.add(new Row("specials", "ARRAY[" + open + "'NaN'::DOUBLE, 'Infinity'::DOUBLE, '-Infinity'::DOUBLE, -0.0" + close + "]"));
    }

    private static void addDecimal(ObjList<Row> rows, int precision, int scale, String cast) {
        final StringBuilder max = new StringBuilder();
        for (int i = 0, n = precision - scale; i < n; i++) {
            max.append('9');
        }
        if (max.isEmpty()) {
            max.append('0');
        }
        if (scale > 0) {
            max.append('.');
            for (int i = 0; i < scale; i++) {
                max.append('9');
            }
        }
        rows.add(new Row("min", "'-" + max + "'" + cast));
        rows.add(new Row("max", "'" + max + "'" + cast));
    }

    private static void addDerivedRows(TypeConformanceTypes.Entry type, ObjList<Row> rows) {
        final TypeDriver driver = ColumnType.getTypeDriver(type.columnType);
        if (!(driver instanceof FixedSizeTypeDriver fixed)) {
            throw new IllegalStateException("type " + type.label + " is var-size: its definition must declare its value rows");
        }
        final int width = fixed.getWidth();
        addTierRows(type, width, rows);
        rows.add(Row.bits("zero", width, 0, 0, 0, 0));
        rows.add(Row.bits("one", width, 1, 0, 0, 0));
        rows.add(Row.bits("ones", width, -1, -1, -1, -1));
        rows.add(Row.bits("sentinel", width, driver.getNullLong(0), driver.getNullLong(1), driver.getNullLong(2), driver.getNullLong(3)));
        // every sentinel of an existing type of the same width, written as a value: for a
        // full-range type these are the legacy sentinels (#6921), for example LONG_MIN and NaN
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (TypeConformanceTypes.PSEUDO_TAGS.contains(tag) || tag == type.tag || tag == ColumnTypeTag.VARCHAR_SLICE) {
                continue;
            }
            final TypeDriver other = ColumnType.getTypeDriver(tag.code());
            if (!(other instanceof FixedSizeTypeDriver otherFixed) || otherFixed.getWidth() != width || other.getNullPolicy() != NullPolicy.SENTINEL) {
                continue;
            }
            final Row row = Row.bits("sentinel_" + tag.name(), width, other.getNullLong(0), other.getNullLong(1), other.getNullLong(2), other.getNullLong(3));
            boolean isNew = true;
            for (int i = 0, n = rows.size(); i < n; i++) {
                isNew &= !Arrays.equals(rows.getQuick(i).bits, row.bits);
            }
            if (isNew) {
                rows.add(row);
            }
        }
    }

    /**
     * Rows the arithmetic tier of a later type's definition implies: the tier's minimum and
     * maximum, and for float tiers NaN, the infinities and -0.0, as raw bits.
     */
    private static void addTierRows(TypeConformanceTypes.Entry type, int width, ObjList<Row> rows) {
        if (type.laterTier == null) {
            return;
        }
        switch (type.laterTier) {
            case "F32" -> {
                rows.add(Row.bits("min", width, Float.floatToRawIntBits(-Float.MAX_VALUE), 0, 0, 0));
                rows.add(Row.bits("max", width, Float.floatToRawIntBits(Float.MAX_VALUE), 0, 0, 0));
                rows.add(Row.bits("nan", width, Float.floatToRawIntBits(Float.NaN), 0, 0, 0));
                rows.add(Row.bits("inf", width, Float.floatToRawIntBits(Float.POSITIVE_INFINITY), 0, 0, 0));
                rows.add(Row.bits("ninf", width, Float.floatToRawIntBits(Float.NEGATIVE_INFINITY), 0, 0, 0));
                rows.add(Row.bits("negzero", width, Float.floatToRawIntBits(-0.0f), 0, 0, 0));
            }
            case "F64" -> {
                rows.add(Row.bits("min", width, Double.doubleToRawLongBits(-Double.MAX_VALUE), 0, 0, 0));
                rows.add(Row.bits("max", width, Double.doubleToRawLongBits(Double.MAX_VALUE), 0, 0, 0));
                rows.add(Row.bits("nan", width, Double.doubleToRawLongBits(Double.NaN), 0, 0, 0));
                rows.add(Row.bits("inf", width, Double.doubleToRawLongBits(Double.POSITIVE_INFINITY), 0, 0, 0));
                rows.add(Row.bits("ninf", width, Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY), 0, 0, 0));
                rows.add(Row.bits("negzero", width, Double.doubleToRawLongBits(-0.0), 0, 0, 0));
            }
            default -> {
                // integer tiers: I<bits> signed, U<bits> unsigned
                final boolean isSigned = type.laterTier.startsWith("I");
                final long min = isSigned ? 1L << (width * 8 - 1) : 0;
                final long max = isSigned ? ~min : -1;
                rows.add(Row.bits("min", width, min, width > 8 ? -1 : 0, width > 8 ? -1 : 0, width > 8 ? -1 : 0));
                rows.add(Row.bits("max", width, max, width > 8 ? -1 : 0, width > 8 ? -1 : 0, width > 8 ? -1 : 0));
            }
        }
    }

    private static void addFloat(ObjList<Row> rows, String min, String max, String cast) {
        rows.add(new Row("min", min));
        rows.add(new Row("max", max));
        // NaN is the sentinel of FLOAT and DOUBLE
        rows.add(new Row("nan", "'NaN'" + cast));
        // SQL stores every non-finite value as NaN; the raw rows inf and ninf store infinities
        rows.add(new Row("literal_inf", "'Infinity'" + cast));
        rows.add(new Row("negzero", "(-0.0)" + cast));
    }

    private static void addGeoHash(ObjList<Row> rows, int bits) {
        final StringBuilder min = new StringBuilder();
        final StringBuilder max = new StringBuilder();
        if (bits % 5 == 0) {
            min.append('#');
            max.append('#');
            for (int i = 0, n = bits / 5; i < n; i++) {
                min.append('0');
                max.append('z');
            }
        } else {
            min.append("##");
            max.append("##");
            for (int i = 0; i < bits; i++) {
                min.append('0');
                max.append('1');
            }
        }
        rows.add(new Row("min", min.toString()));
        rows.add(new Row("max", max.toString()));
    }

    private static void addIntegral(ObjList<Row> rows, String min, String max, @Nullable String sentinel, @Nullable String otherNull) {
        rows.add(new Row("min", min));
        rows.add(new Row("max", max));
        if (sentinel != null) {
            rows.add(new Row("sentinel", sentinel));
        }
        if (otherNull != null) {
            rows.add(new Row("other_null", otherNull));
        }
    }

    private static void addText(ObjList<Row> rows, String cast) {
        rows.add(new Row("empty", "''" + cast));
        rows.add(new Row("min", "' '" + cast));
        rows.add(new Row("max", "'\u00fc\u20ac\uD83D\uDE00\uFFFD'" + cast));
        rows.add(new Row("escape", "'a\"b,c\\d''e'" + cast));
    }

    /**
     * One value row: a label, and either a SQL literal (existing types) or a raw bit pattern
     * of up to four longs, least significant first (types registered later).
     */
    public static final class Row {
        public final long[] bits;
        public final String label;
        @Nullable
        public final String literal;
        public final int width;

        Row(String label, @Nullable String literal) {
            this(label, literal, null, 0);
        }

        private Row(String label, @Nullable String literal, long[] bits, int width) {
            this.label = label;
            this.literal = literal;
            this.bits = bits;
            this.width = width;
        }

        public static Row bits(String label, int width, long l0, long l1, long l2, long l3) {
            final long[] bits = {l0, l1, l2, l3};
            // keep only the type's width, so a read back of the same width compares equal
            for (int i = 0; i < 4; i++) {
                final int bytesLeft = width - i * 8;
                if (bytesLeft <= 0) {
                    bits[i] = 0;
                } else if (bytesLeft < 8) {
                    bits[i] &= (1L << (bytesLeft * 8)) - 1;
                }
            }
            return new Row(label, null, bits, width);
        }

        public boolean isNull() {
            return "null".equals(label);
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
