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
            String[] intTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP"};
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
            String[] floatTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP"};
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
    public void testMixedConversionStatesAcrossPartitionsReproduceAliasingBug() throws Exception {
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
