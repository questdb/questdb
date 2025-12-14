/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

/**
 * Tests for LoopingRecordToRowCopier type conversions and edge cases.
 * These tests verify that the loop-based copier handles all type conversions correctly.
 */
public class LoopingRecordToRowCopierTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, false);
    }

    @Test
    public void testBasicInsertWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables with 1000 columns to trigger loop-based copier
            StringBuilder createSql = new StringBuilder("create table src (ts timestamp");
            for (int i = 0; i < 1000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst (ts timestamp");
            for (int i = 0; i < 1000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert test data
            StringBuilder insertSql = new StringBuilder("insert into src values (0");
            for (int i = 0; i < 1000; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Copy using INSERT AS SELECT (will use loop-based copier)
            execute("insert into dst select * from src");

            // Verify data was copied correctly
            assertSql("count\n1\n", "select count(*) from dst");
            assertSql("col0\tcol50\tcol99\n0\t50\t99\n", "select col0, col50, col99 from dst");
        });
    }

    @Test
    public void testBinaryColumnCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b binary) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b binary) timestamp(ts) partition by DAY");

            // Insert binary data using rnd_bin() function
            execute("insert into src select x::timestamp, rnd_bin(10, 20, 1) from long_sequence(2)");
            execute("insert into dst select * from src");

            assertSql("count\n2\n", "select count(*) from dst");
        });
    }

    @Test
    public void testBooleanCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b boolean) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b boolean) timestamp(ts) partition by DAY");

            execute("insert into src values (0, true), (1000, false), (2000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tb
                    1970-01-01T00:00:00.000000Z\ttrue
                    1970-01-01T00:00:00.001000Z\tfalse
                    1970-01-01T00:00:00.002000Z\tfalse
                    """, "dst");
        });
    }

    @Test
    public void testByteToAllIntegralTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte) timestamp(ts) partition by DAY");
            execute("create table dst_short (ts timestamp, s short) timestamp(ts) partition by DAY");
            execute("create table dst_int (ts timestamp, i int) timestamp(ts) partition by DAY");
            execute("create table dst_long (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 127), (1000, -128), (2000, 0)");

            execute("insert into dst_short select * from src");
            execute("insert into dst_int select * from src");
            execute("insert into dst_long select * from src");
            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\t127
                    1970-01-01T00:00:00.001000Z\t-128
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_short");

            assertSql("""
                    ts\ti
                    1970-01-01T00:00:00.000000Z\t127
                    1970-01-01T00:00:00.001000Z\t-128
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_int");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t127
                    1970-01-01T00:00:00.001000Z\t-128
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_long");

            assertSql("""
                    ts\tf
                    1970-01-01T00:00:00.000000Z\t127.0
                    1970-01-01T00:00:00.001000Z\t-128.0
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_float");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t127.0
                    1970-01-01T00:00:00.001000Z\t-128.0
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_double");
        });
    }

    @Test
    public void testCharColumnCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, c char) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, c char) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'a'), (1000, 'Z'), (2000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tc
                    1970-01-01T00:00:00.000000Z\ta
                    1970-01-01T00:00:00.001000Z\tZ
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testCharToStringConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, c char) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, s string) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'X'), (1000, 'Y')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\tX
                    1970-01-01T00:00:00.001000Z\tY
                    """, "dst");
        });
    }

    @Test
    public void testCharToVarcharConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, c char) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'A'), (1000, 'B')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\tA
                    1970-01-01T00:00:00.001000Z\tB
                    """, "dst");
        });
    }

    @Test
    public void testCompareBothImplementations() throws Exception {
        assertMemoryLeak(() -> {
            // Create test tables
            execute("create table src (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst_result (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");

            // Insert test data
            execute("insert into src values " +
                    "(0, 1, 2, 3, 4, 5.5, 6.6, 'test', 'symbol'), " +
                    "(1000, 10, 20, 30, 40, 50.5, 60.6, 'hello', 'world'), " +
                    "(2000, null, null, null, null, null, null, null, null)");

            // Copy (using default threshold - will use loop-based for 9 columns)
            execute("insert into dst_result select * from src");

            // Verify results are correct
            assertSql("count\n3\n", "select count(*) from dst_result");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst_result", LOG);
        });
    }

    @Test
    public void testDateToTimestampConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d date) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, t timestamp) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '2023-06-15T14:30:00.000Z')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tt
                    1970-01-01T00:00:00.000000Z\t2023-06-15T14:30:00.000000Z
                    """, "dst");
        });
    }

    @Test
    public void testDoubleToByteConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d double) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b byte) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 127.0), (1000, -128.0), (2000, 50.5)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tb
                    1970-01-01T00:00:00.000000Z\t127
                    1970-01-01T00:00:00.001000Z\t-128
                    1970-01-01T00:00:00.002000Z\t50
                    """, "dst");
        });
    }

    @Test
    public void testDoubleToFloatConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d double) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, f float) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 3.14159265358979), (1000, -2.71828)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tf
                    1970-01-01T00:00:00.000000Z\t3.1415927
                    1970-01-01T00:00:00.001000Z\t-2.71828
                    """, "dst");
        });
    }

    @Test
    public void testDoubleToIntConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d double) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 42.99), (1000, -17.1)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ti
                    1970-01-01T00:00:00.000000Z\t42
                    1970-01-01T00:00:00.001000Z\t-17
                    """, "dst");
        });
    }

    @Test
    public void testDoubleToLongConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d double) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 9876543210.123), (1000, -1234567890.987)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t9876543210
                    1970-01-01T00:00:00.001000Z\t-1234567890
                    """, "dst");
        });
    }

    @Test
    public void testDoubleToShortConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, d double) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, s short) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 32767.0), (1000, -32768.0), (2000, 1000.5)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\t32767
                    1970-01-01T00:00:00.001000Z\t-32768
                    1970-01-01T00:00:00.002000Z\t1000
                    """, "dst");
        });
    }

    @Test
    public void testFloatToIntConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 3.14), (1000, 99.9), (2000, -5.5)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ti
                    1970-01-01T00:00:00.000000Z\t3
                    1970-01-01T00:00:00.001000Z\t99
                    1970-01-01T00:00:00.002000Z\t-5
                    """, "dst");
        });
    }

    @Test
    public void testFloatToLongConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 123.456), (1000, -789.012)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t123
                    1970-01-01T00:00:00.001000Z\t-789
                    """, "dst");
        });
    }

    @Test
    public void testIPv4Copy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '192.168.1.1'), (1000, '10.0.0.1'), (2000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tip
                    1970-01-01T00:00:00.000000Z\t192.168.1.1
                    1970-01-01T00:00:00.001000Z\t10.0.0.1
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testIntToAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, i int) timestamp(ts) partition by DAY");
            execute("create table dst_long (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            // Use values that won't trigger null behavior (MIN_INT is often treated as null)
            execute("insert into src values (0, 2147483647), (1000, -2147483647), (2000, 0)");

            execute("insert into dst_long select * from src");
            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t2147483647
                    1970-01-01T00:00:00.001000Z\t-2147483647
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_long");

            // Float has limited precision
            assertSql("""
                    ts\tf
                    1970-01-01T00:00:00.000000Z\t2.14748365E9
                    1970-01-01T00:00:00.001000Z\t-2.14748365E9
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_float");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t2.147483647E9
                    1970-01-01T00:00:00.001000Z\t-2.147483647E9
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_double");
        });
    }

    @Test
    public void testLong128Copy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, l long128) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long128) timestamp(ts) partition by DAY");

            // Long128 values can only be inserted via ILP or direct API, not SQL literals
            // Test with null values to verify the column type copy works
            execute("insert into src values (0, null), (1000, null)");
            execute("insert into dst select * from src");

            assertSql("count\n2\n", "select count(*) from dst");
        });
    }

    @Test
    public void testLong256Copy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, l long256) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long256) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testLongToFloatDoubleConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            // Use smaller values that won't overflow when converted through float/double
            // Long.MIN_VALUE and Long.MAX_VALUE cause overflow issues
            execute("insert into src values (0, 123456789012345), (1000, -123456789012345), (2000, 0)");

            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            // Float has very limited precision for large longs
            assertSql("count\n3\n", "select count(*) from dst_float");
            assertSql("count\n3\n", "select count(*) from dst_double");
        });
    }

    @Test
    public void testMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, i int, s string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int, s string) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 'one'), (1000, 2, 'two'), (2000, 3, 'three')");
            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            assertSql("s\none\ntwo\nthree\n", "select s from dst order by i");
        });
    }

    @Test
    public void testNullHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, i int, s string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int, s string, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, null, null, null)");
            execute("insert into dst select * from src");

            assertSql("ts\ti\ts\tsym\n1970-01-01T00:00:00.000000Z\tnull\t\t\n", "dst");
        });
    }

    @Test
    public void testShortToAllIntegralTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, s short) timestamp(ts) partition by DAY");
            execute("create table dst_int (ts timestamp, i int) timestamp(ts) partition by DAY");
            execute("create table dst_long (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 32767), (1000, -32768), (2000, 0)");

            execute("insert into dst_int select * from src");
            execute("insert into dst_long select * from src");
            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            assertSql("""
                    ts\ti
                    1970-01-01T00:00:00.000000Z\t32767
                    1970-01-01T00:00:00.001000Z\t-32768
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_int");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t32767
                    1970-01-01T00:00:00.001000Z\t-32768
                    1970-01-01T00:00:00.002000Z\t0
                    """, "dst_long");

            assertSql("""
                    ts\tf
                    1970-01-01T00:00:00.000000Z\t32767.0
                    1970-01-01T00:00:00.001000Z\t-32768.0
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_float");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t32767.0
                    1970-01-01T00:00:00.001000Z\t-32768.0
                    1970-01-01T00:00:00.002000Z\t0.0
                    """, "dst_double");
        });
    }

    @Test
    public void testStringToIPv4Conversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '192.168.1.100'), (1000, '10.20.30.40')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tip
                    1970-01-01T00:00:00.000000Z\t192.168.1.100
                    1970-01-01T00:00:00.001000Z\t10.20.30.40
                    """, "dst");
        });
    }

    @Test
    public void testStringToSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, str string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'test_symbol')");
            execute("insert into dst select * from src");

            assertSql("ts\tsym\n1970-01-01T00:00:00.000000Z\ttest_symbol\n", "dst");
        });
    }

    @Test
    public void testStringToUuidConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, u uuid) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '550e8400-e29b-41d4-a716-446655440000')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tu
                    1970-01-01T00:00:00.000000Z\t550e8400-e29b-41d4-a716-446655440000
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testStringToVarcharConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'hello world'), (1000, null), (2000, 'test string')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\thello world
                    1970-01-01T00:00:00.001000Z\t
                    1970-01-01T00:00:00.002000Z\ttest string
                    """, "dst");
        });
    }

    @Test
    public void testSymbolToStringConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, s string) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'apple'), (1000, 'banana'), (2000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\tapple
                    1970-01-01T00:00:00.001000Z\tbanana
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testSymbolToVarcharConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'foo'), (1000, 'bar'), (2000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\tfoo
                    1970-01-01T00:00:00.001000Z\tbar
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testTimestampToDateConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, t timestamp) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, d date) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '2023-06-15T14:30:00.123456Z')");
            execute("insert into dst select * from src");

            // Date has millisecond precision, so microseconds are truncated
            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t2023-06-15T14:30:00.123Z
                    """, "dst");
        });
    }

    @Test
    public void testTypeConversionsWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 2, 3, 4, 5.5, 6.6, 'hello', 'world')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tb\ts\ti\tl\tf\td\tstr\tsym
                    1970-01-01T00:00:00.000000Z\t1\t2\t3\t4\t5.5\t6.6\thello\tworld
                    """, "dst");
        });
    }

    @Test
    public void testTypeWidening() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b long, s long, i long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 10, 20, 30)");
            execute("insert into dst select * from src");

            assertSql("ts\tb\ts\ti\n1970-01-01T00:00:00.000000Z\t10\t20\t30\n", "dst");
        });
    }

    @Test
    public void testUuidCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, u uuid) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '550e8400-e29b-41d4-a716-446655440000')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tu
                    1970-01-01T00:00:00.000000Z\t550e8400-e29b-41d4-a716-446655440000
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testUuidToStringConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, s string) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '550e8400-e29b-41d4-a716-446655440000')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\t550e8400-e29b-41d4-a716-446655440000
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testUuidToVarcharConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testVarcharToDateConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, d date) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '2023-06-15T14:30:00.000Z')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t2023-06-15T14:30:00.000Z
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testVarcharToFloatDoubleConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '3.14'), (1000, '-2.71'), (2000, '999.999')");

            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            assertSql("""
                    ts\tf
                    1970-01-01T00:00:00.000000Z\t3.14
                    1970-01-01T00:00:00.001000Z\t-2.71
                    1970-01-01T00:00:00.002000Z\t999.999
                    """, "dst_float");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t3.14
                    1970-01-01T00:00:00.001000Z\t-2.71
                    1970-01-01T00:00:00.002000Z\t999.999
                    """, "dst_double");
        });
    }

    @Test
    public void testVarcharToIPv4Conversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '172.16.0.1'), (1000, '8.8.8.8')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tip
                    1970-01-01T00:00:00.000000Z\t172.16.0.1
                    1970-01-01T00:00:00.001000Z\t8.8.8.8
                    """, "dst");
        });
    }

    @Test
    public void testVarcharToLong256() throws Exception {
        assertMemoryLeak(() -> {
            // Test VARCHAR to LONG256 conversion to verify the cast is safe
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l256 long256) timestamp(ts) partition by DAY");

            // Insert a valid long256 hex value as varchar
            execute("insert into src values (0, '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')");
            execute("insert into src values (1000, null)");

            // Copy using INSERT AS SELECT
            execute("insert into dst select * from src");

            // Verify data was copied correctly
            assertSql("count\n2\n", "select count(*) from dst");
            assertSql("l256\n0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\n",
                    "select l256 from dst order by ts");
        });
    }

    @Test
    public void testVarcharToNumericConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst_int (ts timestamp, i int) timestamp(ts) partition by DAY");
            execute("create table dst_long (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_short (ts timestamp, s short) timestamp(ts) partition by DAY");
            execute("create table dst_byte (ts timestamp, b byte) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '42'), (1000, '-17'), (2000, '100')");

            execute("insert into dst_int select * from src");
            execute("insert into dst_long select * from src");
            execute("insert into dst_short select * from src");
            execute("insert into dst_byte select * from src");

            assertSql("""
                    ts\ti
                    1970-01-01T00:00:00.000000Z\t42
                    1970-01-01T00:00:00.001000Z\t-17
                    1970-01-01T00:00:00.002000Z\t100
                    """, "dst_int");

            assertSql("""
                    ts\tl
                    1970-01-01T00:00:00.000000Z\t42
                    1970-01-01T00:00:00.001000Z\t-17
                    1970-01-01T00:00:00.002000Z\t100
                    """, "dst_long");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\t42
                    1970-01-01T00:00:00.001000Z\t-17
                    1970-01-01T00:00:00.002000Z\t100
                    """, "dst_short");

            assertSql("""
                    ts\tb
                    1970-01-01T00:00:00.000000Z\t42
                    1970-01-01T00:00:00.001000Z\t-17
                    1970-01-01T00:00:00.002000Z\t100
                    """, "dst_byte");
        });
    }

    @Test
    public void testVarcharToSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'symbol_value'), (1000, 'another_value')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tsym
                    1970-01-01T00:00:00.000000Z\tsymbol_value
                    1970-01-01T00:00:00.001000Z\tanother_value
                    """, "dst");
        });
    }

    @Test
    public void testVarcharToUuidConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, u uuid) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            execute("insert into src values (1000, null)");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tu
                    1970-01-01T00:00:00.000000Z\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst");
        });
    }

    @Test
    public void testVeryWideTable() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 2000 columns to ensure loop-based implementation handles it
            StringBuilder createSql = new StringBuilder("create table wide_src (ts timestamp");
            for (int i = 0; i < 2000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table wide_dst (ts timestamp");
            for (int i = 0; i < 2000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert a row
            StringBuilder insertSql = new StringBuilder("insert into wide_src values (0");
            for (int i = 0; i < 2000; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Copy data
            execute("insert into wide_dst select * from wide_src");

            // Verify
            assertSql("count\n1\n", "select count(*) from wide_dst");
            assertSql("col0\tcol1000\tcol1999\n0\t1000\t1999\n", "select col0, col1000, col1999 from wide_dst");
        });
    }
}
