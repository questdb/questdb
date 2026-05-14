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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests lazy column type conversion on parquet partitions.
 * <p>
 * When a column type changes via ALTER TABLE ALTER COLUMN TYPE while a partition is
 * stored in parquet, the Rust parquet decoder converts data on-the-fly during queries.
 * This test verifies that the parquet (Rust) conversion path produces the same results
 * as the native (JNI) conversion path for all supported type pairs.
 * <p>
 * Rust-handled conversions tested here:
 * <ul>
 *     <li>Fixed-to-Fixed: pairs among BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP,
 *         FLOAT, DOUBLE</li>
 *     <li>Var-to-Var: STRING to VARCHAR (UTF-8 kept), VARCHAR to STRING (UTF-8 to UTF-16)</li>
 * </ul>
 * <p>
 * Each test inserts data (including nulls and boundary values) into two identical WAL
 * tables, converts one to parquet, alters the column type on both, and asserts that
 * both produce identical query results.
 */
public class ParquetColumnTypeConversionTest extends AbstractCairoTest {

    @Test
    public void testBooleanToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (true, '2024-01-01T00:00:01.000000Z'),
                    (false, '2024-01-01T00:00:02.000000Z')""";
            for (String target : new String[]{"BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("BOOLEAN", target, values);
            }
        });
    }

    @Test
    public void testCharToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('z', '2024-01-01T00:00:02.000000Z'),
                    ('1', '2024-01-01T00:00:03.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("CHAR", target, values);
            }
        });
    }

    @Test
    public void testByteToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (127, '2024-01-01T00:00:04.000000Z'),
                    (-128, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("BYTE", target, values);
            }
        });
    }

    @Test
    public void testDateToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // DATE stores milliseconds since epoch.
            // '2020-06-15T12:00:00.000Z' = 1_592_222_400_000 ms
            // DATE -> TIMESTAMP scales x1000 (ms -> us), null preserved.
            // Sub-second millisecond precision exercised with .999Z.
            String values = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    ('2020-06-15T12:00:00.999Z', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("DATE", target, values);
            }
        });
    }

    @Test
    public void testDecimalToDecimal() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (12345.6789m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99.9999m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(18, 4)", "DECIMAL(38, 4)", values);
            assertConversion("DECIMAL(18, 4)", "DECIMAL(38, 8)", values);
        });
    }

    @Test
    public void testDecimalToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (12345.6789m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99.9999m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("DECIMAL(18, 4)", target, values);
            }
        });
    }

    /**
     * Exercises Decimal sources across the six physical backing widths (Decimal8 / Decimal16
     * / Decimal32 / Decimal64 / Decimal128 / Decimal256) which map respectively to parquet
     * Int32, Int32, Int32, Int64, FixedLenByteArray(16), FixedLenByteArray(32). Each row
     * lands on a different dispatch arm in {@code decode_int32_dispatch},
     * {@code decode_int64_dispatch} or {@code decode_fixed_len_dispatch}. The Fixed-to-Var
     * conversion path passes the source type to Rust (Java post-converts), so the matched
     * arm is the source {@code ColumnTypeTag::DecimalN} arm at the column's natural width.
     */
    @Test
    public void testDecimalSourceWidthsToString() throws Exception {
        assertMemoryLeak(() -> {
            // Decimal8 (Int32 physical, fits in i8 after scaling)
            assertConversion("DECIMAL(2, 0)", "STRING", """
                    (12m, '2024-01-01T00:00:01.000000Z'),
                    (-99m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(2, 1)", "VARCHAR", """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-9.9m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal16 (Int32 physical, fits in i16 after scaling)
            assertConversion("DECIMAL(4, 0)", "STRING", """
                    (1234m, '2024-01-01T00:00:01.000000Z'),
                    (-9999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(4, 2)", "VARCHAR", """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (-99.99m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal32 (Int32 physical)
            assertConversion("DECIMAL(9, 0)", "STRING", """
                    (123456789m, '2024-01-01T00:00:01.000000Z'),
                    (-987654321m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(9, 3)", "VARCHAR", """
                    (123456.789m, '2024-01-01T00:00:01.000000Z'),
                    (-987654.321m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal64 (Int64 physical)
            assertConversion("DECIMAL(18, 0)", "STRING", """
                    (123456789012345678m, '2024-01-01T00:00:01.000000Z'),
                    (-999999999999999999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal128 (FixedLenByteArray(16))
            assertConversion("DECIMAL(38, 0)", "STRING", """
                    (12345678901234567890123456789012345678m, '2024-01-01T00:00:01.000000Z'),
                    (-99999999999999999999999999999999999999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(38, 4)", "VARCHAR", """
                    (1234567890123456789012345678901234.5678m, '2024-01-01T00:00:01.000000Z'),
                    (-9999999999999999999999999999999999.9999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal256 (FixedLenByteArray(32))
            assertConversion("DECIMAL(76, 0)", "STRING", """
                    (1234567890123456789012345678901234567890123456789012345678901234567890123456m, '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
            assertConversion("DECIMAL(76, 1)", "VARCHAR", """
                    (123456789012345678901234567890123456789012345678901234567890123456789012345.6m, '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
        });
    }

    /**
     * Pins lazy parquet behavior for DOUBLE-&gt;LONG/DATE/TIMESTAMP at the upper i64
     * boundary. The Rust converter in
     * {@code core/rust/qdbr/src/parquet_read/decode.rs} stores the bound as
     * {@code i64::MAX as f64}. Since {@code i64::MAX = 2^63 - 1} requires 63
     * mantissa bits and f64 only has 53, that cast rounds up to {@code 2^63}.
     * A f64 value equal to {@code 2^63} is strictly greater than {@code i64::MAX}
     * but passes the {@code v <= max} guard, then saturates to {@code i64::MAX}
     * under the {@code as} cast — silently producing wrong data instead of the
     * documented NULL sentinel. Differential assertion against the native (JNI)
     * path does not catch this because the C++ kernel has analogous undefined
     * behavior for out-of-range floats, so this test asserts the contract
     * directly: out-of-range floats read back as NULL.
     */
    @Test
    public void testDoubleToLongBoundaryPrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854776e18 == 2^63 exactly in f64. 2^63 is strictly
            // greater than i64::MAX = 2^63 - 1, so the contract is NULL.
            for (String targetType : new String[]{"LONG", "DATE", "TIMESTAMP"}) {
                assertParquetFloatOutOfRangeNull("DOUBLE", targetType, "9.223372036854776e18");
            }
        });
    }

    @Test
    public void testDoubleToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 1e300 overflows FLOAT range (~3.4e38) and all integer types.
            // Infinity/-Infinity are distinct from NaN (null sentinel).
            String values = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (1e300, '2024-01-01T00:00:04.000000Z'),
                    (1.0/0.0, '2024-01-01T00:00:05.000000Z'),
                    (-1.0/0.0, '2024-01-01T00:00:06.000000Z'),
                    (NULL, '2024-01-01T00:00:07.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT"}) {
                assertConversion("DOUBLE", target, values);
            }
        });
    }

    @Test
    public void testFixedToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Fixed→Var path: Rust decodes the source fixed type;
            // Java formats to string after decode.
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("BOOLEAN", target, """
                        (true, '2024-01-01T00:00:01.000000Z'),
                        (false, '2024-01-01T00:00:02.000000Z')""");

                assertConversion("BYTE", target, """
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("SHORT", target, """
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("INT", target, """
                        (42, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("LONG", target, """
                        (42, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("FLOAT", target, """
                        (1.5, '2024-01-01T00:00:01.000000Z'),
                        (0.0, '2024-01-01T00:00:02.000000Z'),
                        (-1.5, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("DOUBLE", target, """
                        (1.5, '2024-01-01T00:00:01.000000Z'),
                        (0.0, '2024-01-01T00:00:02.000000Z'),
                        (-1.5, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("DATE", target, """
                        ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("TIMESTAMP", target, """
                        ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");
            }
        });
    }

    @Test
    public void testFixedToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // →Symbol requires the pre-pass to convert parquet→native
            // before building the symbol map.
            assertConversion("BOOLEAN", "SYMBOL", """
                    (true, '2024-01-01T00:00:01.000000Z'),
                    (false, '2024-01-01T00:00:02.000000Z')""");

            assertConversion("INT", "SYMBOL", """
                    (42, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            assertConversion("LONG", "SYMBOL", """
                    (42, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("DOUBLE", "SYMBOL", """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("DATE", "SYMBOL", """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");

            assertConversion("TIMESTAMP", "SYMBOL", """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
        });
    }

    @Test
    public void testFixedWithAllEncodings() throws Exception {
        assertMemoryLeak(() -> {
            // Encoding-specific decoders (e.g. delta_binary_packed) cannot produce
            // cross-family output. The dispatch must keep the source type during decode
            // and convert afterward via post_convert.
            String intValues = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (42, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String floatValues = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String dateValues = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String tsValues = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";

            // Integer sources support default, plain, rle_dictionary, delta_binary_packed.
            String[] intEncodings = {"default", "plain", "rle_dictionary", "delta_binary_packed"};
            // BOOLEAN target exercises the new Int32/Int64-to-Boolean dispatch arms in
            // decode_int32_dispatch / decode_int64_dispatch for every encoding.
            String[] intTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"};
            for (String encoding : intEncodings) {
                for (String source : new String[]{"BYTE", "SHORT", "INT", "LONG"}) {
                    for (String target : intTargets) {
                        if (source.equals(target)) continue;
                        assertConversionWithEncoding(source, target, intValues, encoding);
                    }
                }
                for (String target : intTargets) {
                    if (!"DATE".equals(target)) {
                        assertConversionWithEncoding("DATE", target, dateValues, encoding);
                    }
                    if (!"TIMESTAMP".equals(target)) {
                        assertConversionWithEncoding("TIMESTAMP", target, tsValues, encoding);
                    }
                }
            }

            // Float sources support default, plain, rle_dictionary (delta_binary_packed is integer-only).
            String[] floatEncodings = {"default", "plain", "rle_dictionary"};
            // BOOLEAN target exercises the range-checked Double-to-Byte/Boolean and Float-to-Byte/Boolean
            // dispatch arms (decode_double_dispatch, decode_other_fixed_dispatch) for every encoding.
            String[] floatTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"};
            for (String encoding : floatEncodings) {
                for (String source : new String[]{"FLOAT", "DOUBLE"}) {
                    for (String target : floatTargets) {
                        if (source.equals(target)) continue;
                        assertConversionWithEncoding(source, target, floatValues, encoding);
                    }
                }
            }
        });
    }

    /**
     * Var-source mirror of {@link #testFixedWithAllEncodings}. STRING and VARCHAR columns
     * stored under each writer-supported parquet encoding must round-trip identically
     * through both the native and lazy-parquet conversion paths. This exercises the
     * encoding axis of {@code decode_byte_array_dispatch} in {@code decode.rs}:
     * <ul>
     *     <li>{@code (RleDictionary | PlainDictionary, _, String/Varchar)}</li>
     *     <li>{@code (DeltaLengthByteArray, _, String/Varchar)}</li>
     * </ul>
     * The Plain and DeltaByteArray dispatch arms only fire for externally produced parquet
     * (the QuestDB writer rejects {@code plain} on var-size columns and does not emit
     * {@code delta_byte_array}) and are out of scope for a writer-driven test.
     */
    @Test
    public void testVarTypesWithAllEncodings() throws Exception {
        assertMemoryLeak(() -> {
            String numericValues = """
                    ('42', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    ('-1', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String floatValues = """
                    ('1.5', '2024-01-01T00:00:01.000000Z'),
                    ('0.0', '2024-01-01T00:00:02.000000Z'),
                    ('-1.5', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String boolValues = """
                    ('true', '2024-01-01T00:00:01.000000Z'),
                    ('false', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String charValues = """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String ipv4Values = """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String uuidValues = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String dateValues = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String tsValues = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String unicodeValues = """
                    ('hello', '2024-01-01T00:00:01.000000Z'),
                    ('café', '2024-01-01T00:00:02.000000Z'),
                    ('日本語', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";

            // Encodings the QuestDB writer can emit for var-size columns. Var-to-fixed and
            // var-to-var conversions both pass the SOURCE var type to the Rust decoder
            // (Java post-converts), so each combination lands on a different dispatch arm.
            // The writer rejects {@code plain} for var-size columns and never emits
            // {@code delta_binary_packed} for them, so we stick to the supported set.
            String[] varEncodings = {"default", "rle_dictionary", "delta_length_byte_array"};
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                String otherVar = "STRING".equals(source) ? "VARCHAR" : "STRING";
                for (String encoding : varEncodings) {
                    // Var → Var transcode (UTF-16 ↔ UTF-8) across each encoding.
                    assertConversionWithEncoding(source, otherVar, unicodeValues, encoding);

                    // Var → Fixed: small integer family
                    for (String target : new String[]{"BYTE", "SHORT", "INT", "LONG"}) {
                        assertConversionWithEncoding(source, target, numericValues, encoding);
                    }
                    // Var → Fixed: float family
                    for (String target : new String[]{"FLOAT", "DOUBLE"}) {
                        assertConversionWithEncoding(source, target, floatValues, encoding);
                    }
                    // Var → Fixed: BOOLEAN (no null sentinel — NULL maps to false).
                    assertConversionWithEncoding(source, "BOOLEAN", boolValues, encoding);
                    // Var → Fixed: CHAR (single code unit). Multi-byte UTF-8 already covered
                    // by testVarcharToCharPreservesNonAsciiCodepoint.
                    assertConversionWithEncoding(source, "CHAR", charValues, encoding);
                    // Var → Fixed: IPv4
                    assertConversionWithEncoding(source, "IPV4", ipv4Values, encoding);
                    // Var → Fixed: UUID
                    assertConversionWithEncoding(source, "UUID", uuidValues, encoding);
                    // Var → Fixed: temporal types
                    assertConversionWithEncoding(source, "DATE", dateValues, encoding);
                    assertConversionWithEncoding(source, "TIMESTAMP", tsValues, encoding);
                }
            }
        });
    }

    /**
     * Pins lazy parquet behavior for FLOAT-&gt;LONG/DATE/TIMESTAMP at the upper i64
     * boundary. The Rust converter in
     * {@code core/rust/qdbr/src/parquet_read/decode.rs} stores the upper bound as
     * {@code i64::MAX as f32}. Since {@code i64::MAX = 2^63 - 1} is not
     * representable in f32 (only 23 mantissa bits available, 63 needed), that
     * cast rounds up to {@code 2^63} - one ULP above {@code i64::MAX}. A f32
     * value equal to {@code 2^63} is strictly greater than {@code i64::MAX} but
     * passes the {@code v <= max} guard, then saturates to {@code i64::MAX} under
     * the {@code as} cast - silently producing wrong data instead of the
     * documented NULL sentinel. The differential helper {@link #assertConversion}
     * does not catch this because the native (JNI) {@code convert_from_type_to_type}
     * kernel has analogous undefined behavior for out-of-range floats. This test
     * asserts the contract directly: floats strictly above {@code i64::MAX} read
     * back as NULL.
     * <p>
     * At the f32 magnitude of {@code 2^63}, adjacent representable values are
     * spaced by {@code 2^40}, so {@code 2^63} is the only f32 in the open
     * interval {@code (i64::MAX, 2^63]}.
     */
    @Test
    public void testFloatToLongBoundaryPrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854776e18 == 2^63 exactly when stored as f32.
            // 2^63 is strictly greater than i64::MAX = 2^63 - 1, so the contract is NULL.
            for (String targetType : new String[]{"LONG", "DATE", "TIMESTAMP"}) {
                assertParquetFloatOutOfRangeNull("FLOAT", targetType, "cast(9.223372036854776e18 as float)");
            }
        });
    }

    @Test
    public void testFloatToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Float-to-integer conversions truncate the fractional part.
            // NaN (null) maps to the target type's null sentinel.
            // Infinity/-Infinity are distinct from NaN (null sentinel).
            String values = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (cast(1.0/0.0 as float), '2024-01-01T00:00:04.000000Z'),
                    (cast(-1.0/0.0 as float), '2024-01-01T00:00:05.000000Z'),
                    (NULL, '2024-01-01T00:00:06.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "DOUBLE"}) {
                assertConversion("FLOAT", target, values);
            }
        });
    }

    @Test
    public void testIntegerToDecimal() throws Exception {
        assertMemoryLeak(() -> {
            assertConversion("BYTE", "DECIMAL(4, 1)", """
                    (127, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-128, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SHORT", "DECIMAL(8, 2)", """
                    (32767, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-32768, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("INT", "DECIMAL(18, 2)", """
                    (2_147_483_647, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            assertConversion("LONG", "DECIMAL(38, 2)", """
                    (1_000_000_000, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
        });
    }

    /**
     * Pins lazy parquet behavior for narrow-int -&gt; narrow-decimal scaling
     * overflow.
     * <p>
     * In {@code convert_fixed_to_decimal} (core/rust/qdbr/src/parquet_read/row_groups.rs)
     * the Rust path scales each value as {@code val * 10^scale} then writes only
     * the low {@code dst_size} bytes. Without a bounds check the high bytes are
     * silently truncated: INT {@code 2_000_000} -&gt; {@code DECIMAL(2, 2)}
     * (Decimal8, 1 byte) computes {@code 200_000_000}, low byte {@code 0x00},
     * displayed as {@code 0.00}; INT {@code 1_000_000} -&gt; {@code DECIMAL(9, 4)}
     * (Decimal32, 4 bytes) computes {@code 10^10}, low 32 bits
     * {@code 1_410_065_408}, displayed as {@code 141006.5408}.
     * <p>
     * The native (JNI) {@code DecimalColumnTypeConverter.convertToDecimal}
     * scales through Decimal256 and validates against the destination precision,
     * so it rejects the ALTER on overflow with {@code CairoException} (the WAL
     * applier swallows the failure and the column stays INT). The differential
     * helper {@link #assertConversion} cannot express that asymmetry, so this
     * test exercises the parquet path directly and asserts the contract: every
     * overflowing row reads back as NULL, never as a truncated value.
     */
    @Test
    public void testIntToNarrowDecimalScalingOverflow() throws Exception {
        assertMemoryLeak(() -> {
            assertParquetIntToDecimalOverflowNull("DECIMAL(2, 2)", "2_000_000");
            assertParquetIntToDecimalOverflowNull("DECIMAL(4, 2)", "1_000_000");
            assertParquetIntToDecimalOverflowNull("DECIMAL(9, 4)", "1_000_000");
        });
    }

    private void assertParquetFloatOutOfRangeNull(String sourceType, String targetType, String floatExpr) throws Exception {
        // CursorPrinter renders INT/LONG null as the literal "null"
        // (Numbers.append special-cases the null sentinel), while DATE/TIMESTAMP
        // null renders as an empty cell (DateFormatUtils/TimestampFormatUtils
        // early-return on MIN_VALUE).
        String nullRendering = switch (targetType.toUpperCase()) {
            case "INT", "LONG" -> "null";
            case "DATE", "TIMESTAMP" -> "";
            default -> throw new IllegalArgumentException("unsupported target: " + targetType);
        };
        try {
            execute("CREATE TABLE pt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO pt VALUES (" + floatExpr + ", '2024-01-01T00:00:01.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            // Out-of-range floats must read back as the target type's NULL sentinel.
            assertSql("val\n" + nullRendering + "\n", "SELECT val FROM pt");
        } finally {
            tryDrop("pt");
        }
    }

    private void assertParquetIntToDecimalOverflowNull(String targetType, String overflowValue) throws Exception {
        try {
            execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO pt VALUES (" + overflowValue + ", '2024-01-01T00:00:01.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            // CursorPrinter renders Decimal*_NULL as an empty cell.
            assertSql("val\n\n", "SELECT val FROM pt");
        } finally {
            tryDrop("pt");
        }
    }

    @Test
    public void testIntToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 256 wraps to 0 in BYTE (256 & 0xFF = 0).
            // 2_147_483_647 (MAX_INT) wraps to -1 in SHORT (0x7FFFFFFF & 0xFFFF).
            // NULL (INT_MIN) maps to the target type's null sentinel.
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (256, '2024-01-01T00:00:04.000000Z'),
                    (2_147_483_647, '2024-01-01T00:00:05.000000Z'),
                    (NULL, '2024-01-01T00:00:06.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("INT", target, values);
            }
        });
    }

    /**
     * Reproduces a corruption in {@code TableWriter.produceNativeFromParquet} on the
     * fixed-to-var arm when the source parquet has more than one row group. Two
     * compounded defects:
     * <ol>
     *     <li>{@code appendBuffer(dstDataFd, dataBuf, dataSize)} writes the full
     *         {@code estimateStringDataSize / estimateVarcharDataSize} estimate to
     *         disk. {@code convertFixedColumnToString / convertFixedColumnToVarchar}
     *         only populates the actual prefix of that buffer, so the trailing
     *         {@code dataSize - actualBytes} bytes are uninitialized memory from
     *         {@code Unsafe.malloc} leaking into the column data file.</li>
     *     <li>The fixed-to-var arm does not track {@code dataVecBytesWritten} across
     *         row groups, unlike the var-to-var arm at the same call site. Aux
     *         entries produced for row groups beyond the first carry offsets that
     *         are relative to the start of their own local data buffer (i.e. zero
     *         for the first entry of each row group), so subsequent row groups
     *         read back the bytes of row group 0.</li>
     * </ol>
     * <p>
     * The default test row group size is 1_000 rows; the existing fixed-to-var
     * coverage uses fewer rows so the entire partition fits in one row group and
     * neither defect is exercised. This test forces a tiny row group size and
     * uses 12 rows with distinct integer values, so the partition spans three row
     * groups. After {@code ALTER COLUMN ... TYPE STRING/VARCHAR} and
     * {@code CONVERT PARTITION TO NATIVE}, every row must read back as its
     * original integer formatted as a string.
     */
    @Test
    public void testIntToStringConvertToNativeMultiRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // 12 rows in one partition, row group size 4 => three row groups.
                    execute("""
                            INSERT INTO pt VALUES
                            (1,    '2024-01-01T00:00:01.000000Z'),
                            (2,    '2024-01-01T00:00:02.000000Z'),
                            (3,    '2024-01-01T00:00:03.000000Z'),
                            (4,    '2024-01-01T00:00:04.000000Z'),
                            (5,    '2024-01-01T00:00:05.000000Z'),
                            (6,    '2024-01-01T00:00:06.000000Z'),
                            (7,    '2024-01-01T00:00:07.000000Z'),
                            (8,    '2024-01-01T00:00:08.000000Z'),
                            (9,    '2024-01-01T00:00:09.000000Z'),
                            (10,   '2024-01-01T00:00:10.000000Z'),
                            (11,   '2024-01-01T00:00:11.000000Z'),
                            (NULL, '2024-01-01T00:00:12.000000Z')""");
                    drainWalQueue();

                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE " + target);
                    drainWalQueue();

                    // Materializes the fixed->var conversion through produceNativeFromParquet,
                    // so subsequent reads hit the native files, not the lazy parquet path.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertSql(
                            """
                                    val\tts
                                    1\t2024-01-01T00:00:01.000000Z
                                    2\t2024-01-01T00:00:02.000000Z
                                    3\t2024-01-01T00:00:03.000000Z
                                    4\t2024-01-01T00:00:04.000000Z
                                    5\t2024-01-01T00:00:05.000000Z
                                    6\t2024-01-01T00:00:06.000000Z
                                    7\t2024-01-01T00:00:07.000000Z
                                    8\t2024-01-01T00:00:08.000000Z
                                    9\t2024-01-01T00:00:09.000000Z
                                    10\t2024-01-01T00:00:10.000000Z
                                    11\t2024-01-01T00:00:11.000000Z
                                    \t2024-01-01T00:00:12.000000Z
                                    """,
                            "SELECT val, ts FROM pt"
                    );
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    @Test
    public void testIpv4ToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    ('255.255.255.255', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("IPv4", target, values);
            }
        });
    }

    @Test
    public void testLongToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 256 wraps to 0 in BYTE, stays 256 in SHORT.
            // 2_147_483_648 overflows INT (becomes -2_147_483_648 via truncation).
            // NULL (LONG_MIN) maps to the target type's null sentinel.
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (256, '2024-01-01T00:00:04.000000Z'),
                    (2_147_483_648, '2024-01-01T00:00:05.000000Z'),
                    (NULL, '2024-01-01T00:00:06.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("LONG", target, values);
            }
        });
    }

    @Test
    public void testShortToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (32767, '2024-01-01T00:00:04.000000Z'),
                    (-32768, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("SHORT", target, values);
            }
        });
    }

    @Test
    public void testStringToChar() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("STRING", "CHAR", values);
            assertConversion("VARCHAR", "CHAR", values);
        });
    }

    /**
     * Asserts VARCHAR-&gt;CHAR conversion on a parquet partition against an absolute
     * oracle. This is a lazy conversion path: the parquet decoder hands back a
     * {@link io.questdb.std.str.Utf8Sequence} and Java takes the first char via
     * io.questdb.cairo.sql.PageFrameMemoryRecord#convertVarToChar, which reads
     * through {@code readVarValueForConversion -&gt; asAsciiCharSequence()}. The latter
     * exposes each raw UTF-8 byte as a char, so a non-ASCII value like 'é'
     * (UTF-8: 0xC3 0xA9) yields {@code charAt(0) == U+00C3} ('Ã') instead of the
     * correct U+00E9. The differential assertion {@code nt==pt} does NOT catch this
     * because io.questdb.cairo.ColumnTypeConverter#convertFromVarcharToFixed
     * uses the same {@code asAsciiCharSequence()} call, so native and parquet produce
     * the same mojibake. Fix: use a proper UTF-8-to-UTF-16 decoder.
     */
    @Test
    public void testVarcharToCharPreservesNonAsciiCodepoint() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('a', '2024-01-01T00:00:01.000000Z'),
                        ('é', '2024-01-01T00:00:02.000000Z'),
                        ('日', '2024-01-01T00:00:03.000000Z')""");
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                execute("ALTER TABLE pt ALTER COLUMN val TYPE CHAR");
                drainWalQueue();

                // Expected: the first UTF-16 code unit of each stored value.
                //   'a' -> 'a'
                //   'é' -> 'é' (U+00E9)
                //   '日' -> '日' (U+65E5)
                // With the UTF-8-as-ASCII bug, non-ASCII rows produce the first UTF-8 byte
                // as a char (e.g. 'é' -> 'Ã'), so this assertion fails while the bug is present.
                assertSql(
                        """
                                val
                                a
                                é
                                日
                                """,
                        "SELECT val FROM pt ORDER BY ts"
                );
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Same absolute-oracle approach for the O3PartitionJob.convertVarColumnToFixed
     * path, which is taken by CONVERT PARTITION TO NATIVE (and by O3 merge into a
     * parquet partition whose column was ALTERed to a fixed type). The previous code
     * accumulated UTF-8 bytes into a Utf8StringSink and exposed them via
     * asAsciiCharSequence(), so the multi-byte sequence for 'é' (0xC3 0xA9) became
     * two Latin-1 chars (U+00C3, U+00A9) and CHAR materialized as 'Ã' on disk.
     * The differential assertConversion helper does not catch this because both
     * native and the lazy parquet read path use a proper UTF-8 decode now, while
     * this materialization path is only reachable through the O3 merge / convert
     * pipeline.
     */
    @Test
    public void testVarcharToCharNonAsciiThroughConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('a', '2024-01-01T00:00:01.000000Z'),
                        ('élite', '2024-01-01T00:00:02.000000Z'),
                        ('日本', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                execute("ALTER TABLE pt ALTER COLUMN val TYPE CHAR");
                drainWalQueue();
                // Materializes the var->fixed conversion through O3PartitionJob, so
                // the bytes on disk are whatever writeFixedParsedValue wrote. The
                // SELECT below reads native files, not the lazy parquet path.
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();

                assertSql(
                        "val\na\né\n日\n\n",
                        "SELECT val FROM pt ORDER BY ts"
                );
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Same absolute-oracle approach for VARCHAR-&gt;VARCHAR/STRING paths that go through
     * the lazy per-row Java conversion. The native path performs a proper UTF-8 decode,
     * so this test pins the expected behaviour regardless of what the peer native path does.
     */
    @Test
    public void testVarcharToStringPreservesNonAsciiUtf8() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('hello', '2024-01-01T00:00:01.000000Z'),
                        ('café naïve', '2024-01-01T00:00:02.000000Z'),
                        ('日本語', '2024-01-01T00:00:03.000000Z'),
                        ('emoji 🦆', '2024-01-01T00:00:04.000000Z')""");
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                drainWalQueue();

                // Expected: the stored UTF-8 bytes decoded as UTF-16 code points.
                // With asAsciiCharSequence(), each UTF-8 byte becomes a char and the
                // output is mojibake (e.g. 'é' -> 'Ã©').
                assertSql(
                        """
                                val
                                hello
                                café naïve
                                日本語
                                emoji 🦆
                                """,
                        "SELECT val FROM pt ORDER BY ts"
                );
            } finally {
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testStringToDecimal() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('12345.6789', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    ('-99.9999', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("STRING", "DECIMAL(18, 4)", values);
            assertConversion("VARCHAR", "DECIMAL(18, 4)", values);
        });
    }

    @Test
    public void testStringToFixed() throws Exception {
        assertMemoryLeak(() -> {
            // Var→Fixed path: Rust decodes the source var type;
            // Java parses to fixed after decode.
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                assertConversion(source, "BOOLEAN", """
                        ('true', '2024-01-01T00:00:01.000000Z'),
                        ('false', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion(source, "BYTE", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "SHORT", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "INT", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "LONG", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "FLOAT", """
                        ('1.5', '2024-01-01T00:00:01.000000Z'),
                        ('0.0', '2024-01-01T00:00:02.000000Z'),
                        ('-1.5', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "DOUBLE", """
                        ('1.5', '2024-01-01T00:00:01.000000Z'),
                        ('0.0', '2024-01-01T00:00:02.000000Z'),
                        ('-1.5', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "DATE", """
                        ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion(source, "TIMESTAMP", """
                        ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");
            }
        });
    }

    @Test
    public void testStringToIpv4() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    ('255.255.255.255', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("STRING", "IPv4", values);
            assertConversion("VARCHAR", "IPv4", values);
        });
    }

    @Test
    public void testStringToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // →Symbol requires the pre-pass to convert parquet→native
            // before building the symbol map.
            String values = """
                    ('hello', '2024-01-01T00:00:01.000000Z'),
                    ('world', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("STRING", "SYMBOL", values);
            assertConversion("VARCHAR", "SYMBOL", values);
        });
    }

    @Test
    public void testStringToTimestampNano() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("""
                            INSERT INTO pt VALUES
                            ('2020-06-15T12:30:00.123456789Z', '2024-01-01T00:00:01.000000Z'),
                            ('1970-01-01T00:00:00.000000001Z', '2024-01-01T00:00:02.000000Z'),
                            (NULL, '2024-01-01T00:00:03.000000Z')""");
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    execute("ALTER TABLE pt ALTER COLUMN val TYPE TIMESTAMP_NS");
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertSql(
                            """
                                    val\tts
                                    2020-06-15T12:30:00.123456789Z\t2024-01-01T00:00:01.000000Z
                                    1970-01-01T00:00:00.000000001Z\t2024-01-01T00:00:02.000000Z
                                    \t2024-01-01T00:00:03.000000Z
                                    """,
                            "SELECT val, ts FROM pt"
                    );
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    @Test
    public void testStringToUuid() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("STRING", "UUID", values);
            assertConversion("VARCHAR", "UUID", values);
        });
    }

    @Test
    public void testStringToUuidWithInvalidValues() throws Exception {
        // Non-UUID strings must produce NULL on the lazy parquet path, matching the native
        // str2Uuid converter (which calls Uuid.checkDashesAndLength first and treats failure
        // as null). Without the length/dash pre-check, Uuid.parseHi/parseLo index past the
        // end of short strings and raise IndexOutOfBoundsException, which the
        // NumericException-only catch in convertVarToUuidHi/Lo does not handle, propagating
        // the exception out of the Record API.
        assertMemoryLeak(() -> {
            String values = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('not-a-uuid', '2024-01-01T00:00:02.000000Z'),
                    ('', '2024-01-01T00:00:03.000000Z'),
                    ('short', '2024-01-01T00:00:04.000000Z'),
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-extra', '2024-01-01T00:00:05.000000Z'),
                    ('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz', '2024-01-01T00:00:06.000000Z'),
                    (NULL, '2024-01-01T00:00:07.000000Z')""";
            assertConversion("STRING", "UUID", values);
            assertConversion("VARCHAR", "UUID", values);
        });
    }

    @Test
    public void testStringToVarchar() throws Exception {
        // Covers the UTF-8/UTF-16 translation across the conversion boundary. Each width
        // class exercises a distinct encoding path and each is a canonical mojibake source
        // if the lazy path (readVarValueForConversion -> asAsciiCharSequence) is hit:
        //   - 2-byte UTF-8 (e, n, u, a with diacritics): 0xC3 0xXX pairs would render as
        //     two Latin-1 chars (e.g. UTF-8 'e-acute' 0xC3 0xA9 -> U+00C3 U+00A9 'A-tilde,
        //     copyright')
        //   - 3-byte UTF-8 (Cyrillic, CJK): 0xE0-0xEF 0x80-0xBF 0x80-0xBF triples
        //   - 4-byte UTF-8 (supplementary plane, emoji): surrogate pair round-trip
        //   - mixed ASCII + non-ASCII in one value, to catch partial-decode bugs
        assertMemoryLeak(() -> assertConversion("STRING", "VARCHAR", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('\u0442\u0435\u0441\u0442', '2024-01-01T00:00:03.000000Z'),
                ('caf\u00e9 na\u00efve \u00fcber', '2024-01-01T00:00:04.000000Z'),
                ('\u65e5\u672c\u8a9e \u4e2d\u6587 \ud55c\uae00', '2024-01-01T00:00:05.000000Z'),
                ('emoji \ud83e\udd86 \ud83d\ude00 mixed', '2024-01-01T00:00:06.000000Z'),
                ('ascii then \u00e9', '2024-01-01T00:00:07.000000Z'),
                (NULL, '2024-01-01T00:00:08.000000Z')"""));
    }

    @Test
    public void testTimestampToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // TIMESTAMP stores microseconds since epoch.
            // '2020-06-15T12:30:00.123456Z' = 1_592_224_200_123_456 us
            // TIMESTAMP -> DATE divides by 1000 (us -> ms), losing sub-ms precision.
            // NULL (LONG_MIN) is preserved across scaling (checked before divide).
            // Sub-millisecond precision: .000001Z = 1 microsecond, .999999Z = max micros.
            String values = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    ('2020-06-15T12:30:00.000001Z', '2024-01-01T00:00:03.000000Z'),
                    ('2020-06-15T12:30:00.999999Z', '2024-01-01T00:00:04.000000Z'),
                    (NULL, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "FLOAT", "DOUBLE"}) {
                assertConversion("TIMESTAMP", target, values);
            }
        });
    }

    @Test
    public void testSymbolToOtherTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Parquet stores SYMBOL as UTF-8 BYTE_ARRAY, decoded as VARCHAR by Rust.
            String stringValues = """
                    ('hello', '2024-01-01T00:00:01.000000Z'),
                    ('world', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("SYMBOL", "STRING", stringValues);
            assertConversion("SYMBOL", "VARCHAR", stringValues);

            // SYMBOL→Fixed: the pre-pass converts parquet→native,
            // then normal Symbol→X conversion runs (parses symbol string to target).
            String smallIntValues = """
                    ('42', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    ('-1', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("SYMBOL", "BYTE", smallIntValues);
            assertConversion("SYMBOL", "SHORT", smallIntValues);
            assertConversion("SYMBOL", "INT", smallIntValues);
            assertConversion("SYMBOL", "LONG", smallIntValues);

            String floatValues = """
                    ('3.14', '2024-01-01T00:00:01.000000Z'),
                    ('0.0', '2024-01-01T00:00:02.000000Z'),
                    ('-1.5', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("SYMBOL", "FLOAT", floatValues);
            assertConversion("SYMBOL", "DOUBLE", floatValues);

            assertConversion("SYMBOL", "BOOLEAN", """
                    ('true', '2024-01-01T00:00:01.000000Z'),
                    ('false', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "CHAR", """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "IPV4", """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "UUID", """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "DATE", """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "TIMESTAMP", """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
        });
    }

    @Test
    public void testUuidToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("UUID", target, values);
            }
        });
    }

    /**
     * Reproduces the aliasing bug where the pool's {@code sourceColumnTypes}
     * {@link io.questdb.std.IntList} is stored by reference in BOTH Record A and Record B
     * via the package-private {@code init(...)} overload in {@link io.questdb.cairo.sql.PageFrameMemoryRecord}
     * (line 1499). When Record B is navigated via {@code recordAt} to a partition whose
     * parquet schema does NOT need conversion, the pool's {@code sourceColumnTypes} is
     * rebuilt in place (see {@code PageFrameMemoryPool.openParquet} at the
     * {@code setAll(readParquetColumnCount, -1)} call). Record A, which is still
     * anchored at a partition that DOES need INT-&gt;STRING lazy conversion, now reads the
     * overwritten mapping and silently skips the conversion, returning raw INT bytes
     * interpreted as native STRING storage -- garbage.
     * <p>
     * Setup:
     * <ul>
     *   <li>Partition 2024-01-01: inserted while column {@code val} is INT, converted to
     *       parquet (parquet physical type = INT32). After the ALTER, the table schema
     *       says STRING, so this partition needs lazy INT-&gt;STRING conversion.</li>
     *   <li>Partition 2024-01-02: inserted AFTER the ALTER, so {@code val} is already
     *       stored natively as STRING; then converted to parquet (parquet stores STRING).
     *       This partition does NOT need any conversion -- {@code sourceColumnTypes[val]==-1}.</li>
     * </ul>
     * Trigger: iterate the cursor to land Record A on partition 2024-01-01, then call
     * {@code recordAt(recordB, rowIdInPartition_2024_01_02)} to navigate Record B to the
     * other partition. Re-reading Record A's {@code getStrA(val)} should still return the
     * correctly-converted INT value as a string, but with the aliasing bug it returns
     * garbage or throws.
     */
    @Test
    public void testRecordABMixedConversionStatesAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Partition 2024-01-01: val stored as INT in parquet (conversion needed after ALTER).
                execute("INSERT INTO pt VALUES (42, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                // Schema change: val is now STRING. Partition 2024-01-01 still stores INT
                // in its parquet file; reads must lazy-convert INT->STRING.
                execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                drainWalQueue();

                // Partition 2024-01-02: val inserted as STRING natively, then parquet-encoded
                // as STRING. This partition does NOT need any conversion at read time.
                execute("INSERT INTO pt VALUES ('hello-from-p2', '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
                drainWalQueue();

                // Baseline: a straight SELECT must see the correctly-converted strings on both partitions.
                assertSql(
                        "val\tts\n42\t2024-01-01T00:00:00.000000Z\nhello-from-p2\t2024-01-02T00:00:00.000000Z\n",
                        "SELECT * FROM pt ORDER BY ts"
                );

                // Now manually drive the cursor so we can interleave Record A iteration with
                // a Record B recordAt() on a different frame. The aliasing bug hits when
                // the pool's sourceColumnTypes is rebuilt for a non-converting partition
                // while Record A is still pointing at the converting one.
                try (
                        RecordCursorFactory factory = select("SELECT val FROM pt ORDER BY ts");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record recordA = cursor.getRecord();
                    final Record recordB = cursor.getRecordB();

                    Assert.assertTrue("expected at least one row", cursor.hasNext());

                    // Capture the rowId while recordA is positioned on partition 1 (INT->STRING).
                    final long rowIdPartition1 = recordA.getRowId();

                    // Read Record A before any Record B traversal -- must be the converted INT.
                    final StringSink beforeBSink = new StringSink();
                    beforeBSink.put(recordA.getStrA(0));
                    TestUtils.assertEquals("42", beforeBSink);

                    // Advance recordA to row in partition 2 to grab its rowId, then rewind
                    // recordA back to the partition-1 row via recordAt.
                    Assert.assertTrue("expected second row", cursor.hasNext());
                    final long rowIdPartition2 = recordA.getRowId();
                    cursor.recordAt(recordA, rowIdPartition1);
                    TestUtils.assertEquals("42", recordA.getStrA(0));

                    // Trigger the aliasing bug: navigate recordB to partition 2.
                    // This rebuilds the pool's sourceColumnTypes in place with partition 2's
                    // mapping (no conversion), silently overwriting the state Record A relies on.
                    cursor.recordAt(recordB, rowIdPartition2);
                    TestUtils.assertEquals("hello-from-p2", recordB.getStrA(0));

                    // Re-read Record A: it is still anchored at partition 1 (rowIdPartition1)
                    // which needs INT->STRING conversion. If the pool's sourceColumnTypes is
                    // aliased between Record A and Record B, partition 2's "no conversion"
                    // mapping clobbers Record A's view and the read returns garbage.
                    //
                    // With the fix (per-record snapshot of sourceColumnTypes), the assertion
                    // below holds. With the current shared-reference code, it fails.
                    final CharSequence afterB = recordA.getStrA(0);
                    final String afterBStr = afterB == null ? "<null>" : afterB.toString();
                    TestUtils.assertEquals(
                            "Record A must still see partition 1's INT->STRING conversion "
                                    + "after Record B navigated to a non-converting partition. "
                                    + "Got: '" + afterBStr + "'",
                            "42",
                            afterBStr
                    );
                }
            } finally {
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testShortToIntColumnTopAndNull() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Pre-ADD-COLUMN rows -- column_top region for column 'v'.
                String preAdd = """
                        INSERT INTO %s(ts) VALUES
                        ('2020-01-01T00:00:00.000Z'),
                        ('2020-01-01T04:00:00.000Z'),
                        ('2020-01-02T00:00:00.000Z')""";
                execute(preAdd.formatted("nt"));
                execute(preAdd.formatted("pt"));
                drainWalQueue();

                execute("ALTER TABLE nt ADD COLUMN v SHORT");
                execute("ALTER TABLE pt ADD COLUMN v SHORT");
                drainWalQueue();

                String postAdd = """
                        INSERT INTO %s(v, ts) VALUES
                        (5, '2020-01-01T08:00:00.000Z'),
                        (NULL, '2020-01-01T12:00:00.000Z'),
                        (7, '2020-01-01T16:00:00.000Z'),
                        (8, '2020-01-01T20:00:00.000Z'),
                        (9, '2020-01-02T12:00:00.000Z')""";
                execute(postAdd.formatted("nt"));
                execute(postAdd.formatted("pt"));
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
                drainWalQueue();

                execute("ALTER TABLE nt ALTER COLUMN v TYPE INT");
                execute("ALTER TABLE pt ALTER COLUMN v TYPE INT");
                drainWalQueue();

                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testDoubleToLongBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854775807E18 parses to the nearest f64, 2^63 (one ULP
            // above Long.MAX_VALUE). Contract: the parquet path must emit NULL
            // rather than letting `as i64` saturate to Long.MAX_VALUE.
            // Asserted via absolute oracle because the native (JNI) path has
            // an analogous precision-loss bug and would diverge from parquet.
            assertParquetFloatOutOfRangeNull("DOUBLE", "LONG", "9.223372036854775807E18");
        });
    }

    @Test
    public void testFloatToIntBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // 2.147483647E9 parses to the nearest f32, 2^31 (one ULP above INT_MAX).
            // Contract: the parquet path must emit NULL rather than letting
            // `as i32` saturate to Integer.MAX_VALUE.
            assertParquetFloatOutOfRangeNull("FLOAT", "INT", "cast(2.147483647E9 as float)");
        });
    }

    @Test
    public void testVarcharToString() throws Exception {
        // VARCHAR is stored as UTF-8 in parquet; STRING is UTF-16 in memory. Non-ASCII
        // must survive the decode intact. If the lazy path (convertVarToStr ->
        // readVarValueForConversion) is ever reached, asAsciiCharSequence() would expose
        // each UTF-8 byte as a char (Latin-1), producing mojibake: e.g. UTF-8 'e-acute'
        // 0xC3 0xA9 would become two UTF-16 code units U+00C3 U+00A9 ('A-tilde,
        // copyright') instead of the single 'e-acute'. Each width class exercises a
        // distinct UTF-8 decode path:
        //   - 2-byte UTF-8: Latin-1 supplement diacritics and Cyrillic
        //   - 3-byte UTF-8: CJK
        //   - 4-byte UTF-8: supplementary plane / emoji (surrogate pairs)
        //   - mixed ASCII + non-ASCII boundary within one value
        assertMemoryLeak(() -> assertConversion("VARCHAR", "STRING", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('\u0442\u0435\u0441\u0442', '2024-01-01T00:00:03.000000Z'),
                ('caf\u00e9 na\u00efve \u00fcber', '2024-01-01T00:00:04.000000Z'),
                ('\u65e5\u672c\u8a9e \u4e2d\u6587 \ud55c\uae00', '2024-01-01T00:00:05.000000Z'),
                ('emoji \ud83e\udd86 \ud83d\ude00 mixed', '2024-01-01T00:00:06.000000Z'),
                ('ascii then \u00e9', '2024-01-01T00:00:07.000000Z'),
                (NULL, '2024-01-01T00:00:08.000000Z')"""));
    }

    /**
     * The async not-keyed group-by factory runs an aggregation without a {@code GROUP BY}
     * clause. Its inner loop checks {@code frameMemory.needsColumnTypeCast()} and falls back
     * to a row-by-row update path when {@code true}; the batched / vectorized aggregation
     * path reads raw page addresses and would silently produce wrong values if the parquet
     * column needs lazy conversion. The control table {@code nt} is native; {@code pt} is a
     * parquet partition with {@code val} ALTER'd from STRING to INT, which sets
     * {@code hasTypeCasts=true} (var to fixed). Results across both tables must match.
     */
    @Test
    public void testAsyncGroupByNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT min(val) mn, max(val) mx, sum(val) s, avg(val) a, count() c FROM $T WHERE val > 50"
        ));
    }

    /**
     * The async keyed group-by factory. Same {@code needsColumnTypeCast()} gate. The
     * difference vs the not-keyed variant is that a {@code GROUP BY} key is materialized
     * per row and the per-key aggregation update path runs.
     */
    @Test
    public void testAsyncGroupByKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT sym, min(val) mn, max(val) mx, sum(val) s, count() c FROM $T WHERE val > 50 GROUP BY sym ORDER BY sym"
        ));
    }

    /**
     * The async top-K factory orders by {@code val} with a {@code LIMIT}. Its vectorized
     * comparator path reads {@code val} via the column page address; with a lazy var to
     * fixed conversion in parquet, only the scalar fallback materializes the converted
     * integer.
     */
    @Test
    public void testAsyncTopKOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT val, sym FROM $T WHERE val > 50 ORDER BY val DESC, sym LIMIT 10"
        ));
    }

    /**
     * The async JIT-filtered factory. When parquet needs a lazy conversion, the JIT'ed
     * filter cannot be applied directly to the parquet page bytes -- the factory must
     * fall back to scalar filter evaluation through {@code PageFrameMemoryRecord}.
     */
    @Test
    public void testAsyncJitFilteredOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT val, sym FROM $T WHERE val > 50 AND val < 150 ORDER BY ts"
        ));
    }

    /**
     * The async horizon-join factory with no {@code GROUP BY} keys -- a single aggregated
     * output row. The left side is the parquet+ALTER'd table; the right side is a native
     * shared {@code prices} table. The aggregation references the lazy-converted {@code val}.
     */
    @Test
    public void testAsyncHorizonJoinNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT count() n, sum(t.val) sum_val, avg(p.price) avg_price
                        FROM $T t
                        HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50"""
        ));
    }

    /**
     * The async horizon-join factory with {@code GROUP BY} keys. Same gate, plus the keyed
     * grouping path is exercised.
     */
    @Test
    public void testAsyncHorizonJoinKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, h.offset, count() n, sum(t.val) sum_val, avg(p.price) avg_price
                        FROM $T t
                        HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50
                        GROUP BY t.sym, h.offset
                        ORDER BY t.sym, h.offset"""
        ));
    }

    /**
     * The async multi-horizon-join factory with no {@code GROUP BY} keys -- a single
     * aggregated output row. Two HORIZON JOIN clauses share a single offset {@code LIST},
     * which routes the query through {@code AsyncMultiHorizonJoinNotKeyedRecordCursorFactory}.
     * The {@code WHERE t.val > 50} predicate is JIT-compiled against the current INT
     * metadata, but the parquet frame still stores {@code val} as STRING. Without the
     * {@code needsColumnTypeCast()} fallback the compiled filter would read VARCHAR aux
     * bytes as INT and select wrong rows -- the parity check against the native control
     * pins the bug.
     */
    @Test
    public void testAsyncMultiHorizonJoinNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncMultiJoinFactoryParity(
                """
                        SELECT count() n, sum(t.val) sum_val, avg(b.price) avg_bid, avg(a.price) avg_ask
                        FROM $T t
                        HORIZON JOIN bids b ON (t.sym = b.sym)
                        HORIZON JOIN asks a ON (t.sym = a.sym)
                            LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50"""
        ));
    }

    /**
     * The async multi-horizon-join factory with {@code GROUP BY} keys. Same gate as the
     * not-keyed variant; selects {@code AsyncMultiHorizonJoinRecordCursorFactory}.
     */
    @Test
    public void testAsyncMultiHorizonJoinKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncMultiJoinFactoryParity(
                """
                        SELECT t.sym, h.offset, count() n, sum(t.val) sum_val,
                               avg(b.price) avg_bid, avg(a.price) avg_ask
                        FROM $T t
                        HORIZON JOIN bids b ON (t.sym = b.sym)
                        HORIZON JOIN asks a ON (t.sym = a.sym)
                            LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50
                        GROUP BY t.sym, h.offset
                        ORDER BY t.sym, h.offset"""
        ));
    }

    /**
     * The async window-join FAST factory ({@code WINDOW JOIN ... ON (key)}). The left
     * frame is the parquet+ALTER'd table; the join key is the symbol column. Aggregation
     * references both the lazy-converted left column {@code t.val} and the right table's
     * {@code p.price}.
     */
    @Test
    public void testAsyncWindowJoinFastOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, t.val, t.ts, sum(p.price) p_sum
                        FROM $T t
                        WINDOW JOIN prices p ON (t.sym = p.sym)
                        RANGE BETWEEN 30 SECONDS PRECEDING AND 30 SECONDS FOLLOWING EXCLUDE PREVAILING
                        WHERE t.val > 50
                        ORDER BY t.ts, t.sym"""
        ));
    }

    /**
     * The async window-join SLOW factory ({@code WINDOW JOIN} without {@code ON}). Same
     * fallback gate as the fast variant.
     */
    @Test
    public void testAsyncWindowJoinOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, t.val, t.ts, sum(p.price) p_sum
                        FROM $T t
                        WINDOW JOIN prices p
                        RANGE BETWEEN 30 SECONDS PRECEDING AND 30 SECONDS FOLLOWING EXCLUDE PREVAILING
                        WHERE t.val > 50
                        ORDER BY t.ts, t.sym"""
        ));
    }

    /**
     * Verifies snapshot isolation across {@code ALTER TABLE ... ALTER COLUMN TYPE} on a
     * parquet-backed partition. A cursor opened against the pre-ALTER transaction must
     * continue to see the original column type and values for its entire lifetime, while
     * a fresh cursor opened after the ALTER has been applied via the WAL must see the
     * converted type. This pins the contract between the page frame pool's
     * {@code sourceColumnTypes} snapshot, the parquet frame cache, and cursor state
     * across a schema change.
     */
    @Test
    public void testReaderOpenedBeforeAlterSeesOldSchema() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (2, '2024-01-01T00:00:02.000000Z'),
                        (3, '2024-01-01T00:00:03.000000Z')""");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                try (
                        RecordCursorFactory oldFactory = select("SELECT val FROM pt ORDER BY ts");
                        RecordCursor oldCursor = oldFactory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertEquals(
                            ColumnType.INT,
                            ColumnType.tagOf(oldFactory.getMetadata().getColumnType(0))
                    );

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                    drainWalQueue();

                    // The pre-ALTER cursor still sees INT values from its snapshot.
                    Record oldRecord = oldCursor.getRecord();
                    int expected = 1;
                    while (oldCursor.hasNext()) {
                        Assert.assertEquals("row " + expected, expected, oldRecord.getInt(0));
                        expected++;
                    }
                    Assert.assertEquals(4, expected);

                    // A fresh cursor opened after the ALTER sees the converted STRING values.
                    try (
                            RecordCursorFactory newFactory = select("SELECT val FROM pt ORDER BY ts");
                            RecordCursor newCursor = newFactory.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertEquals(
                                ColumnType.STRING,
                                ColumnType.tagOf(newFactory.getMetadata().getColumnType(0))
                        );
                        Record newRecord = newCursor.getRecord();
                        StringSink sink = new StringSink();
                        int newExpected = 1;
                        while (newCursor.hasNext()) {
                            sink.clear();
                            sink.put(newRecord.getStrA(0));
                            TestUtils.assertEquals(Integer.toString(newExpected), sink);
                            newExpected++;
                        }
                        Assert.assertEquals(4, newExpected);
                    }

                    // Re-read the held cursor: still INT, still the original values.
                    oldCursor.toTop();
                    expected = 1;
                    while (oldCursor.hasNext()) {
                        Assert.assertEquals("re-read row " + expected, expected, oldRecord.getInt(0));
                        expected++;
                    }
                    Assert.assertEquals(4, expected);
                }
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Verifies that the parquet (Rust) conversion path matches the native (JNI) path.
     * <ol>
     *     <li>Creates two identical WAL tables: {@code nt} (native reference) and {@code pt} (parquet under test)</li>
     *     <li>Inserts the same test data into both</li>
     *     <li>Converts the {@code pt} partition to parquet</li>
     *     <li>Alters the column type on both tables</li>
     *     <li>Asserts both produce identical query results</li>
     * </ol>
     */
    /**
     * Builds a native control table {@code nt} and a parquet-backed table {@code pt} with
     * identical data, then ALTERs {@code val} from STRING to INT on both. On {@code pt} the
     * parquet keeps the STRING storage so reads go through lazy var to fixed conversion,
     * setting {@code needsColumnTypeCast()=true} on every parquet frame. The supplied
     * query template (with {@code $T} placeholder) runs against both tables and must
     * produce identical cursors.
     */
    private void assertAsyncFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insert = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(200)""";
            execute(insert.replace("$T", "nt"));
            execute(insert.replace("$T", "pt"));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Same as {@link #assertAsyncFactoryParity(String)} but also builds a shared native
     * {@code prices} table used as the right side of HORIZON JOIN / WINDOW JOIN queries.
     */
    private void assertAsyncJoinFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE prices (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insertLeft = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(100)""";
            execute(insertLeft.replace("$T", "nt"));
            execute(insertLeft.replace("$T", "pt"));
            execute("""
                    INSERT INTO prices
                    SELECT (x * 1.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000) AS ts
                    FROM long_sequence(200)""");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
            tryDrop("prices");
        }
    }

    /**
     * Same as {@link #assertAsyncJoinFactoryParity(String)} but builds two native slave
     * tables ({@code bids} and {@code asks}) so MULTI HORIZON JOIN queries route through
     * the {@code AsyncMultiHorizonJoin(NotKeyed)RecordCursorFactory} pair.
     * <p>
     * Spreads data across many daily partitions on the master side. The compiled-filter
     * fallback under {@code needsColumnTypeCast()} only fires when {@link SelectivityStats}
     * decides against late materialization (selectivity above 20% with at least two
     * recorded samples). A single-partition setup keeps every frame on the late-material
     * path, where {@code hasColumnTops()} already routes around the compiled filter and
     * the cast bug stays hidden. Spreading rows over ~20 days produces enough frames per
     * worker for the SelectivityStats EMA to disable late materialization on subsequent
     * frames.
     */
    private void assertAsyncMultiJoinFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bids (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE asks (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insertLeft = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000_000L) AS ts
                    FROM long_sequence(200)""";
            execute(insertLeft.replace("$T", "nt"));
            execute(insertLeft.replace("$T", "pt"));
            execute("""
                    INSERT INTO bids
                    SELECT (x * 1.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000_000L) AS ts
                    FROM long_sequence(400)""");
            execute("""
                    INSERT INTO asks
                    SELECT (x * 2.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000_000L) AS ts
                    FROM long_sequence(400)""");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
            tryDrop("bids");
            tryDrop("asks");
        }
    }

    private void assertConversion(String sourceType, String targetType, String values) throws Exception {
        assertConversionWithEncoding(sourceType, targetType, values, null);
    }

    private void assertConversionWithEncoding(String sourceType, String targetType, String values, String encoding) throws Exception {
        try {
            execute("CREATE TABLE nt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO nt VALUES " + values);
            execute("INSERT INTO pt VALUES " + values);
            drainWalQueue();

            if (encoding != null) {
                execute("ALTER TABLE pt ALTER COLUMN val SET PARQUET(" + encoding + ")");
                drainWalQueue();
            }

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();

            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    private void tryDrop(String tableName) {
        try {
            execute("DROP TABLE " + tableName);
        } catch (Exception ignored) {
        }
    }
}
