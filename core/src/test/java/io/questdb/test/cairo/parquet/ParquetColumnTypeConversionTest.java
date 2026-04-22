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

import io.questdb.test.AbstractCairoTest;
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
    public void testStringToVarchar() throws Exception {
        assertMemoryLeak(() -> assertConversion("STRING", "VARCHAR", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('\u0442\u0435\u0441\u0442', '2024-01-01T00:00:03.000000Z'),
                (NULL, '2024-01-01T00:00:04.000000Z')"""));
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
            String numericValues = """
                    ('42', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("SYMBOL", "INT", numericValues);
            assertConversion("SYMBOL", "LONG", numericValues);
            assertConversion("SYMBOL", "DOUBLE", numericValues);

            assertConversion("SYMBOL", "BOOLEAN", """
                    ('true', '2024-01-01T00:00:01.000000Z'),
                    ('false', '2024-01-01T00:00:02.000000Z'),
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

    @Test
    public void testVarcharToString() throws Exception {
        assertMemoryLeak(() -> assertConversion("VARCHAR", "STRING", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('\u0442\u0435\u0441\u0442', '2024-01-01T00:00:03.000000Z'),
                (NULL, '2024-01-01T00:00:04.000000Z')"""));
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
    private void assertConversion(String sourceType, String targetType, String values) throws Exception {
        try {
            execute("CREATE TABLE nt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO nt VALUES " + values);
            execute("INSERT INTO pt VALUES " + values);
            drainWalQueue();

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
