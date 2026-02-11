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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Integration tests for RecordToRowCopier implementations.
 * These tests verify that bytecode-based, chunked, and loop-based implementations
 * produce identical results across various scenarios.
 * <p>
 * The test is parameterized to run with three different copier modes:
 * - BYTECODE: Single-method bytecode generation (small tables, chunking disabled)
 * - CHUNKED: Chunked bytecode generation (wide tables, chunking enabled)
 * - LOOPING: Loop-based fallback (wide tables, chunking disabled)
 */
@RunWith(Parameterized.class)
public class RecordToRowCopierIntegrationTest extends AbstractCairoTest {

    private final CopierMode copierMode;

    public RecordToRowCopierIntegrationTest(CopierMode copierMode) {
        this.copierMode = copierMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CopierMode.BYTECODE},
                {CopierMode.CHUNKED},
                {CopierMode.LOOPING}
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        // Configure based on copier mode
        switch (copierMode) {
            case BYTECODE:
            case LOOPING:
                // Disable chunking - bytecode for small tables, looping for wide tables
                node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, false);
                break;
            case CHUNKED:
                // Enable chunking for wide tables
                node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, true);
                break;
        }
    }

    /**
     * Tests all primitive numeric types: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE
     */
    @Test
    public void testAllNumericTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "b byte, " +
                    "s short, " +
                    "i int, " +
                    "l long, " +
                    "f float, " +
                    "d double" +
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "b byte, " +
                    "s short, " +
                    "i int, " +
                    "l long, " +
                    "f float, " +
                    "d double" +
                    ") timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 1, 2, 3, 4, 5.5, 6.6), " +
                    "(1000000, -128, -32768, -2147483648, cast(-9223372036854775808 as long), -1.5, -2.5), " +
                    "(2000000, 127, 32767, 2147483647, cast(9223372036854775807 as long), 3.14159, 2.71828), " +
                    "(3000000, null, null, null, null, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests all types together in a single wide table.
     * For LOOPING mode, we need enough columns to exceed 8KB bytecode limit.
     * With 15 columns per multiplier iteration, we need 500/15 ≈ 34 iterations.
     */
    @Test
    public void testAllTypesWideTable() throws Exception {
        int columnMultiplier = copierMode == CopierMode.BYTECODE ? 2 : 35;
        assertMemoryLeak(() -> {
            // Build source table with all types repeated
            sink.clear();
            sink.put("create table src (ts timestamp");
            for (int i = 0; i < columnMultiplier; i++) {
                sink.put(", b").put(i).put(" byte");
                sink.put(", s").put(i).put(" short");
                sink.put(", i").put(i).put(" int");
                sink.put(", l").put(i).put(" long");
                sink.put(", f").put(i).put(" float");
                sink.put(", d").put(i).put(" double");
                sink.put(", c").put(i).put(" char");
                sink.put(", bl").put(i).put(" boolean");
                sink.put(", str").put(i).put(" string");
                sink.put(", vc").put(i).put(" varchar");
                sink.put(", sym").put(i).put(" symbol");
                sink.put(", dt").put(i).put(" date");
                sink.put(", ts").put(i).put(" timestamp");
                sink.put(", ip").put(i).put(" ipv4");
                sink.put(", u").put(i).put(" uuid");
            }
            sink.put(") timestamp(ts) partition by year");
            execute(sink);

            // Build destination table with same schema
            String createDst = sink.toString().replace("src", "dst");
            execute(createDst);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < columnMultiplier; i++) {
                sink.put(", ").put(i % 128);           // byte
                sink.put(", ").put(i * 100);           // short
                sink.put(", ").put(i * 1000);          // int
                sink.put(", ").put(i * 10000L);        // long
                sink.put(", ").put(i * 1.5);           // float
                sink.put(", ").put(i * 2.5);           // double
                sink.put(", 'A'");                     // char
                sink.put(", ").put(i % 2 == 0);        // boolean
                sink.put(", 'str").put(i).put("'");    // string
                sink.put(", 'vc").put(i).put("'");     // varchar
                sink.put(", 'sym").put(i % 5).put("'"); // symbol
                sink.put(", '2024-01-").put(String.format("%02d", (i % 28) + 1)).put("'"); // date
                sink.put(", ").put(i * 1000000L);      // timestamp
                sink.put(", '192.168.").put(i % 256).put(".1'"); // ipv4
                sink.put(", '").put(String.format("%08d", i)).put("-0000-0000-0000-").put(String.format("%012d", i)).put("'"); // uuid
            }
            sink.put(")");
            execute(sink);

            // Copy data
            execute("insert into dst select * from src");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testBatchInsertWithManyColumns() throws Exception {
        int columnCount = getColumnCount(200);
        assertMemoryLeak(() -> {
            // Create source table using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000 as timestamp) as ts");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", cast((x - 1 + ").put(col).put(") % 1000 as int) as col").put(col);
            }
            sink.put(" from long_sequence(1000)) timestamp(ts) partition by year");
            execute(sink);

            buildCreateTableSql("dst", columnCount);
            execute(sink);

            // Batch copy
            execute("insert into dst select * from src");

            assertSql("count\n1000\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests BINARY type
     */
    @Test
    public void testBinaryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b binary) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, b binary) timestamp(ts) partition by year");

            // Insert some binary data using functions
            execute("insert into src select cast(x * 1000000 as timestamp) as ts, " +
                    "case when x % 2 = 0 then null else rnd_bin(10, 100, 2) end as b " +
                    "from long_sequence(10)");

            execute("insert into dst select * from src");

            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests char conversions to various types
     */
    @Test
    public void testCharConversions() throws Exception {
        assertMemoryLeak(() -> {
            // Char -> String
            execute("create table src1 (ts timestamp, c char) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, c string) timestamp(ts) partition by year");
            execute("insert into src1 values (0, 'A'), (1000000, 'Z')");
            execute("insert into dst1 select * from src1");
            assertSql("c\nA\nZ\n", "select c from dst1 order by ts");

            // Char -> Varchar
            execute("create table src2 (ts timestamp, c char) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, c varchar) timestamp(ts) partition by year");
            execute("insert into src2 values (0, 'X'), (1000000, 'Y')");
            execute("insert into dst2 select * from src2");
            assertSql("c\nX\nY\n", "select c from dst2 order by ts");

            // Char -> Symbol (without nulls since char null handling may differ)
            execute("create table src3 (ts timestamp, c char) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, c symbol) timestamp(ts) partition by year");
            execute("insert into src3 values (0, 'M'), (1000000, 'N')");
            execute("insert into dst3 select * from src3");
            assertSql("c\nM\nN\n", "select c from dst3 order by ts");
        });
    }

    /**
     * Tests CHAR type with various characters including special cases
     */
    @Test
    public void testCharType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, c char) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, c char) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 'A'), " +
                    "(1000000, 'Z'), " +
                    "(2000000, '0'), " +
                    "(3000000, ' '), " +
                    "(4000000, null)");

            execute("insert into dst select * from src");

            assertSql("count\n5\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectVeryWideTable() throws Exception {
        int columnCount = getColumnCount(500);
        assertMemoryLeak(() -> {
            buildCreateTableSqlWithTypes("src", columnCount, i -> "int");
            execute(sink);

            // Insert a row
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", ").put(i);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            int lastCol = columnCount - 1;
            int midCol = columnCount / 2;
            assertSql("c0\tc" + midCol + "\tc" + lastCol + "\n0\t" + midCol + "\t" + lastCol + "\n",
                    "select c0, c" + midCol + ", c" + lastCol + " from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithManyColumns() throws Exception {
        int columnCount = getColumnCount(200);
        assertMemoryLeak(() -> {
            buildCreateTableSql("src", columnCount);
            execute(sink);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", ").put(col);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithManyColumnsAndRows() throws Exception {
        int columnCount = getColumnCount(100);
        assertMemoryLeak(() -> {
            // Create source table using long_sequence
            sink.clear();
            sink.put("create table src as (select cast(x * 1000000 as timestamp) as ts");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", cast((x * 100 + ").put(col).put(") % 10000 as int) as col").put(col);
            }
            sink.put(" from long_sequence(100)) timestamp(ts) partition by year");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n100\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithMixedTypes() throws Exception {
        int typeCount = getColumnCount(90) / 3; // 3 columns per type (int, long, double)
        assertMemoryLeak(() -> {
            // Create source table with many columns of various types
            sink.clear();
            sink.put("create table src (ts timestamp");
            for (int i = 0; i < typeCount; i++) {
                sink.put(", int_col").put(i).put(" int");
                sink.put(", long_col").put(i).put(" long");
                sink.put(", double_col").put(i).put(" double");
            }
            sink.put(") timestamp(ts) partition by year");
            execute(sink);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < typeCount; i++) {
                sink.put(", ").put(i);       // int
                sink.put(", ").put(i * 100L); // long
                sink.put(", ").put(i * 1.5);  // double
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithNulls() throws Exception {
        int columnCount = getColumnCount(100);
        assertMemoryLeak(() -> {
            buildCreateTableSql("src", columnCount);
            execute(sink);

            // Insert rows with null values
            sink.clear();
            sink.put("insert into src values (0");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", null");
            }
            sink.put(")");
            execute(sink);

            sink.clear();
            sink.put("insert into src values (1000000");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", ").put(col);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n2\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithPartitionAndFilter() throws Exception {
        int columnCount = getColumnCount(50);
        assertMemoryLeak(() -> {
            // Create source table with many columns using long_sequence
            // Space rows across different days (86400000000 microseconds = 1 day)
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 86400000000L as timestamp) as ts");
            for (int col = 0; col < columnCount; col++) {
                sink.put(", cast((x - 1) * 100 + ").put(col).put(" as int) as col").put(col);
            }
            sink.put(" from long_sequence(20)) timestamp(ts) partition by year");
            execute(sink);

            // Create table using SELECT with WHERE clause
            execute("create table dst as (select * from src where col0 >= 500)");

            // Verify - should have rows 5-19 (15 rows total)
            assertSql("count\n15\n", "select count(*) from dst");
        });
    }

    @Test
    public void testCreateTableAsSelectWithStringsAndSymbols() throws Exception {
        // Use smaller base count since we create 2 columns per iteration (string + symbol)
        // This still exceeds 8KB bytecode limit: 225 iterations * 2 cols * ~20 bytes = 9KB
        int columnCount = getColumnCount(50) / 2;
        assertMemoryLeak(() -> {
            // Create source table with string and symbol columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000000 as timestamp) as ts");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", concat('str', x - 1, '_', ").put(i).put(") as str_col").put(i);
                sink.put(", cast(concat('sym', (x - 1) % 5) as symbol) as sym_col").put(i);
            }
            sink.put(" from long_sequence(10)) timestamp(ts)");
            execute(sink);


            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithTypeWidening() throws Exception {
        int columnCount = getColumnCount(50);
        assertMemoryLeak(() -> {
            // Create source table with narrower types
            sink.clear();
            sink.put("create table src (ts timestamp");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", byte_col").put(i).put(" byte");
                sink.put(", short_col").put(i).put(" short");
            }
            sink.put(") timestamp(ts) partition by year");
            execute(sink);

            // Insert test data - use modulo to stay within type limits
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", ").put(i % 127);             // byte: max 127
                sink.put(", ").put((i * 10) % 32000);    // short: max 32767
            }
            sink.put(")");
            execute(sink);

            // Create table with wider types using CAST
            sink.clear();
            sink.put("create table dst as (select ts");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", cast(byte_col").put(i).put(" as long) as long_from_byte").put(i);
                sink.put(", cast(short_col").put(i).put(" as long) as long_from_short").put(i);
            }
            sink.put(" from src)");
            execute(sink);

            // Verify data was copied correctly
            assertSql("count\n1\n", "select count(*) from dst");

            // Spot check some values
            int checkIdx = Math.min(25, columnCount - 1);
            String expected = "long_from_byte0\tlong_from_byte" + checkIdx + "\tlong_from_short0\tlong_from_short" + checkIdx + "\n" +
                    "0\t" + (checkIdx % 127) + "\t0\t" + ((checkIdx * 10) % 32000) + "\n";
            assertSql(expected,
                    "select long_from_byte0, long_from_byte" + checkIdx + ", long_from_short0, long_from_short" + checkIdx + " from dst");
        });
    }

    /**
     * Tests DECIMAL types at various precisions
     */
    @Test
    public void testDecimalTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Use DECIMAL32 and DECIMAL64 which are more commonly used
            execute("create table src (" +
                    "ts timestamp, " +
                    "d9_4 decimal(9,4), " +    // DECIMAL32
                    "d18_6 decimal(18,6)" +     // DECIMAL64
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "d9_4 decimal(9,4), " +
                    "d18_6 decimal(18,6)" +
                    ") timestamp(ts) partition by year");

            // Use integer values which are more reliable for decimal insertion
            execute("insert into src values " +
                    "(0, 12345, 123456789), " +
                    "(1000000, -12345, -123456789), " +
                    "(2000000, 0, 0), " +
                    "(3000000, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testExtremelyWideTableInsert() throws Exception {
        int columnCount = getColumnCount(100);
        assertMemoryLeak(() -> {
            buildCreateTableSqlWithTypes("src_extreme", columnCount, i -> "int");
            execute(sink);

            buildCreateTableSqlWithTypes("dst_extreme", columnCount, i -> "int");
            execute(sink);

            // Insert a single row with all values
            sink.clear();
            sink.put("insert into src_extreme values (0");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", ").put(i);
            }
            sink.put(")");
            execute(sink);

            // Copy it
            execute("insert into dst_extreme select * from src_extreme");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst_extreme");
            int midCol = columnCount / 2;
            int lastCol = columnCount - 1;
            assertSql("c0\tc" + midCol + "\tc" + lastCol + "\n0\t" + midCol + "\t" + lastCol + "\n",
                    "select c0, c" + midCol + ", c" + lastCol + " from dst_extreme");
        });
    }

    /**
     * Tests GeoHash narrowing conversions (larger to smaller)
     */
    @Test
    public void testGeoHashNarrowing() throws Exception {
        assertMemoryLeak(() -> {
            // GEOLONG to GEOINT
            execute("create table src1 (ts timestamp, g geohash(12c)) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, g geohash(6c)) timestamp(ts) partition by year");
            execute("insert into src1 values (0, 'u33d8b121234'), (1000000, 'sp052w92p1p8')");
            execute("insert into dst1 select * from src1");
            assertSql("g\nu33d8b\nsp052w\n", "select g from dst1 order by ts");

            // GEOINT to GEOSHORT
            execute("create table src2 (ts timestamp, g geohash(6c)) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, g geohash(3c)) timestamp(ts) partition by year");
            execute("insert into src2 values (0, 'u33d8b'), (1000000, 'sp052w')");
            execute("insert into dst2 select * from src2");
            assertSql("g\nu33\nsp0\n", "select g from dst2 order by ts");

            // GEOSHORT to GEOBYTE
            execute("create table src3 (ts timestamp, g geohash(3c)) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, g geohash(1c)) timestamp(ts) partition by year");
            execute("insert into src3 values (0, 'u33'), (1000000, 'sp0')");
            execute("insert into dst3 select * from src3");
            assertSql("g\nu\ns\n", "select g from dst3 order by ts");
        });
    }

    /**
     * Tests all GeoHash types: GEOBYTE, GEOSHORT, GEOINT, GEOLONG
     */
    @Test
    public void testGeoHashTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "g1 geohash(1c), " +   // GEOBYTE (5 bits = 1 char)
                    "g2 geohash(3c), " +   // GEOSHORT (15 bits = 3 chars)
                    "g4 geohash(6c), " +   // GEOINT (30 bits = 6 chars)
                    "g8 geohash(12c)" +    // GEOLONG (60 bits = 12 chars)
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "g1 geohash(1c), " +
                    "g2 geohash(3c), " +
                    "g4 geohash(6c), " +
                    "g8 geohash(12c)" +
                    ") timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 'u', 'u33', 'u33d8b', 'u33d8b121234'), " +
                    "(1000000, 's', 'sp0', 'sp052w', 'sp052w92p1p8'), " +
                    "(2000000, null, null, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    // ==================== COMPREHENSIVE TYPE COVERAGE TESTS ====================

    /**
     * Tests IPv4 type
     */
    @Test
    public void testIPv4Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, ip ipv4) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, ip ipv4) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, '192.168.1.1'), " +
                    "(1000000, '10.0.0.1'), " +
                    "(2000000, '255.255.255.255'), " +
                    "(3000000, '0.0.0.0'), " +
                    "(4000000, null)");

            execute("insert into dst select * from src");

            assertSql("count\n5\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithComplexTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 1, 2, 3.14, 'hello', 'world', true, '2024-01-01'), " +
                    "(1000000, 10, 20, 2.71, 'foo', 'bar', false, '2024-02-01'), " +
                    "(2000000, null, null, null, null, null, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithManyRows() throws Exception {
        int columnCount = getColumnCount(50);
        assertMemoryLeak(() -> {
            buildCreateTableSql("src", columnCount);
            execute(sink);

            buildCreateTableSql("dst", columnCount);
            execute(sink);

            // Insert test data with multiple rows
            buildBatchInsertValuesSql(columnCount);
            execute(sink);

            // Copy data
            execute("insert into dst select * from src");

            // Verify
            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithPartitionedTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, val int) timestamp(ts) partition by MONTH");
            execute("create table dst (ts timestamp, val int) timestamp(ts) partition by MONTH");

            // Insert data across multiple months
            execute("insert into src values " +
                    "('2024-01-15', 1), " +
                    "('2024-02-15', 2), " +
                    "('2024-03-15', 3), " +
                    "('2024-04-15', 4)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithStringSymbolConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src_str (ts timestamp, val string) timestamp(ts) partition by year");
            execute("create table dst_sym (ts timestamp, val symbol) timestamp(ts) partition by year");

            execute("insert into src_str values (0, 'apple'), (1000000, 'banana'), (2000000, 'cherry')");
            execute("insert into dst_sym select * from src_str");

            assertSql("val\napple\nbanana\ncherry\n", "select val from dst_sym order by ts");

            // Test reverse direction
            execute("create table src_sym (ts timestamp, val symbol) timestamp(ts) partition by year");
            execute("create table dst_str (ts timestamp, val string) timestamp(ts) partition by year");

            execute("insert into src_sym values (0, 'red'), (1000000, 'green'), (2000000, 'blue')");
            execute("insert into dst_str select * from src_sym");

            assertSql("val\nred\ngreen\nblue\n", "select val from dst_str order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithTypeConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, b long, s long, i long) timestamp(ts) partition by year");

            execute("insert into src values (0, 1, 2, 3), (1000000, 10, 20, 30), (2000000, 100, 200, 300)");
            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            assertSql("b\ts\ti\n1\t2\t3\n10\t20\t30\n100\t200\t300\n", "select b, s, i from dst order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithUnionExcept() throws Exception {
        int columnCount = getColumnCount(100);
        assertMemoryLeak(() -> {
            buildCreateTableSql("t1", columnCount);
            execute(sink);

            buildCreateTableSql("t2", columnCount);
            execute(sink);

            buildCreateTableSql("result", columnCount);
            execute(sink);

            // Insert data
            buildInsertValuesSql("t1", 0, columnCount, 0);
            execute(sink);

            buildInsertValuesSql("t2", 1000000, columnCount, 100);
            execute(sink);

            // Test UNION
            execute("insert into result select * from t1 union select * from t2");
            assertSql("count\n2\n", "select count(*) from result");
        });
    }

    @Test
    public void testInsertAsSelectWithWhere() throws Exception {
        int columnCount = getColumnCount(50);
        assertMemoryLeak(() -> {
            // Create source table with 'id' column + regular columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000000 as timestamp) as ts, cast(x - 1 as int) as id");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", cast((x - 1) * 100 + ").put(i).put(" as int) as col").put(i);
            }
            sink.put(" from long_sequence(10)) timestamp(ts) partition by year");
            execute(sink);

            sink.clear();
            sink.put("create table dst (ts timestamp, id int");
            for (int i = 0; i < columnCount; i++) {
                sink.put(", col").put(i).put(" int");
            }
            sink.put(") timestamp(ts) partition by year");
            execute(sink);

            // Insert only rows where id > 5
            execute("insert into dst select * from src where id > 5");

            assertSql("count\n4\n", "select count(*) from dst");
            assertSql("id\n6\n7\n8\n9\n", "select id from dst order by id");
        });
    }

    /**
     * Tests integer to decimal conversions
     */
    @Test
    public void testIntegerToDecimalConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "b byte, " +
                    "s short, " +
                    "i int, " +
                    "l long" +
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "b decimal(10,2), " +
                    "s decimal(10,2), " +
                    "i decimal(15,2), " +
                    "l decimal(20,2)" +
                    ") timestamp(ts) partition by year");

            // Skip null test since null handling differs for type conversions
            execute("insert into src values " +
                    "(0, 42, 1234, 123456, 12345678901234), " +
                    "(1000000, -42, -1234, -123456, -12345678901234), " +
                    "(2000000, 0, 0, 0, 0)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            // Verify values manually since types differ
            assertSql("""
                            b\ts\ti\tl
                            42.00\t1234.00\t123456.00\t12345678901234.00
                            -42.00\t-1234.00\t-123456.00\t-12345678901234.00
                            0.00\t0.00\t0.00\t0.00
                            """,
                    "select b, s, i, l from dst order by ts");
        });
    }

    /**
     * Tests LONG256 type
     */
    @Test
    public void testLong256Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, l long256) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, l long256) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, '0x01'), " +
                    "(1000000, '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'), " +
                    "(2000000, '0x0'), " +
                    "(3000000, null)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests many columns with mixed types to stress test all three copier modes.
     * For LOOPING mode, we need 500+ columns to exceed 8KB bytecode limit.
     * With 5 columns per iteration (int, long, double, string, symbol), need 100 iterations.
     */
    @Test
    public void testManyColumnsAllTypes() throws Exception {
        int baseCount = copierMode == CopierMode.BYTECODE ? 5 : 100;
        assertMemoryLeak(() -> {
            // Create tables with repeated type patterns
            sink.clear();
            sink.put("create table src as (select cast(x * 1000000 as timestamp) as ts");
            for (int i = 0; i < baseCount; i++) {
                sink.put(", cast(x as int) as i").put(i);
                sink.put(", cast(x as long) as l").put(i);
                sink.put(", cast(x * 1.5 as double) as d").put(i);
                sink.put(", concat('s', x) as str").put(i);
                sink.put(", cast(concat('sym', x % 5) as symbol) as sym").put(i);
            }
            sink.put(" from long_sequence(50)) timestamp(ts) partition by year");
            execute(sink);

            execute("create table dst as (select * from src where 1=0)");

            execute("insert into dst select * from src");

            assertSql("count\n50\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests type widening conversions: smaller numeric types to larger ones
     */
    @Test
    public void testNumericWidening() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "b byte, " +
                    "s short, " +
                    "i int" +
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "b long, " +      // byte -> long
                    "s long, " +      // short -> long
                    "i long" +        // int -> long
                    ") timestamp(ts) partition by year");

            // Test non-null values only since null handling may differ
            execute("insert into src values " +
                    "(0, 1, 2, 3), " +
                    "(1000000, -128, -32768, -100000), " +
                    "(2000000, 127, 32767, 2147483647)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            // Verify specific values
            assertSql("""
                            b\ts\ti
                            1\t2\t3
                            -128\t-32768\t-100000
                            127\t32767\t2147483647
                            """,
                    "select b, s, i from dst order by ts");
        });
    }

    /**
     * Tests string parsing to various types
     */
    @Test
    public void testStringParsingConversions() throws Exception {
        assertMemoryLeak(() -> {
            // String -> numeric types
            execute("create table src1 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, v int) timestamp(ts) partition by year");
            execute("insert into src1 values (0, '123'), (1000000, '-456'), (2000000, null)");
            execute("insert into dst1 select * from src1");
            assertSql("v\n123\n-456\nnull\n", "select v from dst1 order by ts");

            // String -> timestamp
            execute("create table src2 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, v timestamp) timestamp(ts) partition by year");
            execute("insert into src2 values (0, '2024-01-15T10:30:45.123456'), (1000000, '2024-06-20T23:59:59.999999')");
            execute("insert into dst2 select * from src2");
            assertSql("count\n2\n", "select count(*) from dst2");

            // String -> UUID
            execute("create table src3 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, v uuid) timestamp(ts) partition by year");
            execute("insert into src3 values (0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), (1000000, null)");
            execute("insert into dst3 select * from src3");
            assertSql("v\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n\n", "select v from dst3 order by ts");
        });
    }

    /**
     * Tests String/Symbol/Varchar conversions
     */
    @Test
    public void testStringTypeConversions() throws Exception {
        assertMemoryLeak(() -> {
            // String -> Symbol
            execute("create table src1 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, v symbol) timestamp(ts) partition by year");
            execute("insert into src1 values (0, 'apple'), (1000000, 'banana'), (2000000, null)");
            execute("insert into dst1 select * from src1");
            assertSql("v\napple\nbanana\n\n", "select v from dst1 order by ts");

            // Symbol -> String
            execute("create table src2 (ts timestamp, v symbol) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("insert into src2 values (0, 'red'), (1000000, 'green'), (2000000, null)");
            execute("insert into dst2 select * from src2");
            assertSql("v\nred\ngreen\n\n", "select v from dst2 order by ts");

            // String -> Varchar
            execute("create table src3 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("insert into src3 values (0, 'hello'), (1000000, 'world'), (2000000, null)");
            execute("insert into dst3 select * from src3");
            assertSql("v\nhello\nworld\n\n", "select v from dst3 order by ts");

            // Varchar -> String
            execute("create table src4 (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("create table dst4 (ts timestamp, v string) timestamp(ts) partition by year");
            execute("insert into src4 values (0, 'foo'), (1000000, 'bar'), (2000000, null)");
            execute("insert into dst4 select * from src4");
            assertSql("v\nfoo\nbar\n\n", "select v from dst4 order by ts");
        });
    }

    /**
     * Tests temporal type conversions
     */
    @Test
    public void testTemporalConversions() throws Exception {
        assertMemoryLeak(() -> {
            // DATE -> TIMESTAMP
            execute("create table src1 (ts timestamp, d date) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, d timestamp) timestamp(ts) partition by year");
            execute("insert into src1 values (0, '2024-01-15'), (1000000, '2024-06-20')");
            execute("insert into dst1 select * from src1");
            assertSql("count\n2\n", "select count(*) from dst1");

            // TIMESTAMP -> DATE
            execute("create table src2 (ts timestamp, t timestamp) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, t date) timestamp(ts) partition by year");
            execute("insert into src2 values (0, '2024-01-15T10:30:45.123456'), (1000000, '2024-06-20T23:59:59.999999')");
            execute("insert into dst2 select * from src2");
            assertSql("count\n2\n", "select count(*) from dst2");

            // TIMESTAMP -> LONG
            execute("create table src3 (ts timestamp, t timestamp) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, t long) timestamp(ts) partition by year");
            execute("insert into src3 values (0, '2024-01-15T00:00:00.000000'), (1000000, '2024-01-15T00:00:01.000000')");
            execute("insert into dst3 select * from src3");
            assertSql("t\n1705276800000000\n1705276801000000\n", "select t from dst3 order by ts");
        });
    }

    /**
     * Tests TIMESTAMP with both MICRO and NANO precision
     */
    @Test
    public void testTimestampPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "ts_micro timestamp, " +
                    "ts_nano timestamp_ns" +
                    ") timestamp(ts) partition by year");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "ts_micro timestamp, " +
                    "ts_nano timestamp_ns" +
                    ") timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, '2024-01-15T10:30:45.123456', '2024-01-15T10:30:45.123456789'), " +
                    "(1000000, '2024-06-20T23:59:59.999999', '2024-06-20T23:59:59.999999999'), " +
                    "(2000000, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests UUID to String conversion
     */
    @Test
    public void testUuidToStringConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, u string) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), " +
                    "(1000000, '11111111-2222-3333-4444-555555555555'), " +
                    "(2000000, null)");

            execute("insert into dst select * from src");

            assertSql("u\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n11111111-2222-3333-4444-555555555555\n\n",
                    "select u from dst order by ts");
        });
    }

    /**
     * Tests UUID type
     */
    @Test
    public void testUuidType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, u uuid) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, '11111111-1111-1111-1111-111111111111'), " +
                    "(1000000, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), " +
                    "(2000000, '00000000-0000-0000-0000-000000000000'), " +
                    "(3000000, 'ffffffff-ffff-ffff-ffff-ffffffffffff'), " +
                    "(4000000, null)");

            execute("insert into dst select * from src");

            assertSql("count\n5\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Tests varchar parsing to various types
     */
    @Test
    public void testVarcharParsingConversions() throws Exception {
        assertMemoryLeak(() -> {
            // Varchar -> numeric types
            execute("create table src1 (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("create table dst1 (ts timestamp, v long) timestamp(ts) partition by year");
            execute("insert into src1 values (0, '9876543210'), (1000000, '-9876543210'), (2000000, null)");
            execute("insert into dst1 select * from src1");
            assertSql("v\n9876543210\n-9876543210\nnull\n", "select v from dst1 order by ts");

            // Varchar -> double
            execute("create table src2 (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("create table dst2 (ts timestamp, v double) timestamp(ts) partition by year");
            execute("insert into src2 values (0, '3.14159'), (1000000, '-2.71828'), (2000000, null)");
            execute("insert into dst2 select * from src2");
            assertSql("v\n3.14159\n-2.71828\nnull\n", "select v from dst2 order by ts");

            // Varchar -> IPv4
            execute("create table src3 (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("create table dst3 (ts timestamp, v ipv4) timestamp(ts) partition by year");
            execute("insert into src3 values (0, '192.168.1.1'), (1000000, '10.0.0.1'), (2000000, null)");
            execute("insert into dst3 select * from src3");
            assertSql("v\n192.168.1.1\n10.0.0.1\n\n", "select v from dst3 order by ts");
        });
    }

    /**
     * Tests VARCHAR type
     */
    @Test
    public void testVarcharType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, v varchar) timestamp(ts) partition by year");
            execute("create table dst (ts timestamp, v varchar) timestamp(ts) partition by year");

            execute("insert into src values " +
                    "(0, 'hello'), " +
                    "(1000000, 'world'), " +
                    "(2000000, ''), " +
                    "(3000000, 'a very long string that should test the varchar handling properly'), " +
                    "(4000000, null)");

            execute("insert into dst select * from src");

            assertSql("count\n5\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    /**
     * Builds an INSERT VALUES SQL statement with multiple rows.
     */
    private static void buildBatchInsertValuesSql(
            int columnCount
    ) {
        AbstractCairoTest.sink.clear();
        AbstractCairoTest.sink.put("insert into ").put("src").put(" values ");
        for (int row = 0; row < 10; row++) {
            if (row > 0) {
                AbstractCairoTest.sink.put(", ");
            }
            AbstractCairoTest.sink.put('(').put(row * 1000000L);
            for (int col = 0; col < columnCount; col++) {
                AbstractCairoTest.sink.put(", ").put(row * 100 + col);
            }
            AbstractCairoTest.sink.put(')');
        }
    }

    /**
     * Builds a CREATE TABLE SQL statement with the specified columns.
     */
    private static void buildCreateTableSql(String tableName, int columnCount) {
        AbstractCairoTest.sink.clear();
        AbstractCairoTest.sink.put("create table ").put(tableName).put(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            AbstractCairoTest.sink.put(", col").put(i).put(" int");
        }
        AbstractCairoTest.sink.put(") timestamp(ts) partition by year");
    }

    /**
     * Builds a CREATE TABLE SQL statement with custom column definitions.
     */
    private static void buildCreateTableSqlWithTypes(
            String tableName,
            int columnCount,
            ColumnTypeProvider typeProvider
    ) {
        AbstractCairoTest.sink.clear();
        AbstractCairoTest.sink.put("create table ").put(tableName).put(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            AbstractCairoTest.sink.put(", ").put("c").put(i).put(" ").put(typeProvider.getType(i));
        }
        AbstractCairoTest.sink.put(") timestamp(ts) partition by year");
    }

    /**
     * Builds an INSERT VALUES SQL statement with a single row.
     */
    private static void buildInsertValuesSql(
            String tableName,
            long timestamp,
            int columnCount,
            int valueOffset
    ) {
        AbstractCairoTest.sink.clear();
        AbstractCairoTest.sink.put("insert into ").put(tableName).put(" values (").put(timestamp);
        for (int i = 0; i < columnCount; i++) {
            AbstractCairoTest.sink.put(", ").put(valueOffset + i);
        }
        AbstractCairoTest.sink.put(")");
    }

    /**
     * Returns the number of columns to use based on copier mode.
     * - BYTECODE mode uses fewer columns to stay under bytecode limit (~50 cols)
     * - CHUNKED mode uses many columns but chunking splits them into methods
     * - LOOPING mode needs 400+ columns to exceed 8KB bytecode limit and trigger fallback
     * (BASE_BYTECODE_PER_COLUMN=20 bytes, so 8000/20=400 columns minimum)
     * Note: Keep column count low enough to avoid exhausting file handles on macOS during full test runs.
     */
    private int getColumnCount(int baseCount) {
        return switch (copierMode) {
            case BYTECODE -> Math.min(baseCount, 50);  // Stay under bytecode limit
            case LOOPING, CHUNKED -> Math.max(baseCount, 450); // Must exceed 8KB limit (450*20=9KB > 8KB)
        };
    }

    /**
     * Copier implementation modes to test.
     */
    public enum CopierMode {
        /**
         * Single-method bytecode generation for small schemas.
         * Uses chunkedEnabled=false with small column counts.
         */
        BYTECODE,
        /**
         * Chunked bytecode generation for large schemas.
         * Uses chunkedEnabled=true with large column counts.
         */
        CHUNKED,
        /**
         * Loop-based fallback implementation.
         * Uses chunkedEnabled=false with large column counts.
         */
        LOOPING
    }

    @FunctionalInterface
    private interface ColumnTypeProvider {
        String getType(int columnIndex);
    }
}