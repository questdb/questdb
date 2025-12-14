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
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for RecordToRowCopier implementations.
 * These tests verify that all copier types (single-method, chunked, looping) produce identical results
 * by randomly selecting copier types and comparing outputs.
 */
public class LoopingRecordToRowCopierTest extends AbstractCairoTest {

    private static final int[] COPIER_TYPES = {
            RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD,
            RecordToRowCopierUtils.COPIER_TYPE_CHUNKED,
            RecordToRowCopierUtils.COPIER_TYPE_LOOPING
    };

    @Test
    public void testCompareAllCopiersWithWideTable() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int colCount = 50 + rnd.nextInt(50); // 50-100 columns
            int rowCount = 5 + rnd.nextInt(15);  // 5-20 rows

            // Build column definitions
            StringBuilder cols = new StringBuilder("ts timestamp");
            for (int i = 0; i < colCount; i++) {
                cols.append(", col").append(i).append(" int");
            }

            execute("create table src (" + cols + ") timestamp(ts) partition by DAY");
            execute("create table dst_loop (" + cols + ") timestamp(ts) partition by DAY");
            execute("create table dst_single (" + cols + ") timestamp(ts) partition by DAY");
            execute("create table dst_chunk (" + cols + ") timestamp(ts) partition by DAY");

            // Build insert with random values
            String insertVals = "select x::timestamp" + ", rnd_int()".repeat(Math.max(0, colCount)) +
                    " from long_sequence(" + rowCount + ")";
            execute("insert into src " + insertVals);

            // Copy with each copier type
            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_LOOPING);
            execute("insert into dst_loop select * from src");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD);
            execute("insert into dst_single select * from src");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_CHUNKED);
            execute("insert into dst_chunk select * from src");

            // All should match
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst_loop order by ts", "dst_single order by ts", LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst_loop order by ts", "dst_chunk order by ts", LOG);
        });
    }

    @Test
    public void testFuzzAllTypesLoopingVsChunked() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzAllTypes(rnd, RecordToRowCopierUtils.COPIER_TYPE_LOOPING, RecordToRowCopierUtils.COPIER_TYPE_CHUNKED);
    }

    @Test
    public void testFuzzAllTypesLoopingVsSingleMethod() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzAllTypes(rnd, RecordToRowCopierUtils.COPIER_TYPE_LOOPING, RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD);
    }

    @Test
    public void testFuzzAllTypesSingleMethodVsChunked() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzAllTypes(rnd, RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD, RecordToRowCopierUtils.COPIER_TYPE_CHUNKED);
    }

    @Test
    public void testFuzzBinaryColumn() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            execute("create table src (ts timestamp, b binary) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b binary) timestamp(ts) partition by DAY");

            execute("insert into src select x::timestamp, rnd_bin(10, 20, 1) from long_sequence(5)");
            execute("insert into dst select * from src");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src order by ts", "dst order by ts", LOG);
        });
    }

    @Test
    public void testFuzzCharToStringConversion() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType1 = randomCopierType(rnd);
            int copierType2 = randomCopierType(rnd);

            execute("create table src (ts timestamp, c char) timestamp(ts) partition by DAY");
            execute("create table dst1 (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst1_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst2 (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst2_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'A'), (1000, 'Z'), (2000, null)");

            setCopierType(copierType1);
            execute("insert into dst1 select * from src");
            execute("insert into dst1_vc select * from src");

            setCopierType(copierType2);
            execute("insert into dst2 select * from src");
            execute("insert into dst2_vc select * from src");

            // Compare results
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1 order by ts", "dst2 order by ts", LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1_vc order by ts", "dst2_vc order by ts", LOG);
        });
    }

    @Test
    public void testFuzzDateTimestampConversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // Timestamp to Date (loses microseconds)
            execute("create table src_ts (ts timestamp, t timestamp) timestamp(ts) partition by DAY");
            execute("create table dst_date (ts timestamp, d date) timestamp(ts) partition by DAY");

            execute("insert into src_ts values (0, '2023-06-15T14:30:00.123456Z')");
            execute("insert into dst_date select * from src_ts");

            assertSql("""
                    ts\td
                    1970-01-01T00:00:00.000000Z\t2023-06-15T14:30:00.123Z
                    """, "dst_date");

            // Date to Timestamp
            execute("create table src_date (ts timestamp, d date) timestamp(ts) partition by DAY");
            execute("create table dst_ts (ts timestamp, t timestamp) timestamp(ts) partition by DAY");

            execute("insert into src_date values (0, '2023-06-15T14:30:00.000Z')");
            execute("insert into dst_ts select * from src_date");

            assertSql("""
                    ts\tt
                    1970-01-01T00:00:00.000000Z\t2023-06-15T14:30:00.000000Z
                    """, "dst_ts");
        });
    }

    @Test
    public void testFuzzDecimalTypes() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            execute("create table src_dec (ts timestamp, d decimal(18,3)) timestamp(ts) partition by DAY");
            execute("create table dst_dec (ts timestamp, d decimal(18,3)) timestamp(ts) partition by DAY");

            // Insert using cast to decimal
            execute("insert into src_dec select 0::timestamp, 123.456::decimal(18,3)");
            execute("insert into src_dec select 1000::timestamp, (-789.012)::decimal(18,3)");
            execute("insert into src_dec select 2000::timestamp, null::decimal(18,3)");
            execute("insert into dst_dec select * from src_dec");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src_dec order by ts", "dst_dec order by ts", LOG);
        });
    }

    @Test
    public void testFuzzGeoHashNulls() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // Use geohash sizes that match the bit counts exactly
            execute("create table src (ts timestamp, g4 geohash(4b), g10 geohash(10b), g20 geohash(20b), g40 geohash(40b)) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, g4 geohash(4b), g10 geohash(10b), g20 geohash(20b), g40 geohash(40b)) timestamp(ts) partition by DAY");

            // Insert nulls
            execute("insert into src values (0, null, null, null, null)");
            // Insert valid values using rnd_geohash which generates exact bit counts
            execute("insert into src select 1000::timestamp, rnd_geohash(4), rnd_geohash(10), rnd_geohash(20), rnd_geohash(40)");
            execute("insert into dst select * from src");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src order by ts", "dst order by ts", LOG);
        });
    }

    @Test
    public void testFuzzIPv4Conversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // String to IPv4
            execute("create table src_str (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_ipv4 (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");

            execute("insert into src_str values (0, '192.168.1.1'), (1000, '10.0.0.1')");
            execute("insert into dst_ipv4 select * from src_str");

            assertSql("""
                    ts\tip
                    1970-01-01T00:00:00.000000Z\t192.168.1.1
                    1970-01-01T00:00:00.001000Z\t10.0.0.1
                    """, "dst_ipv4");

            // Varchar to IPv4
            execute("create table src_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst_ipv4_2 (ts timestamp, ip ipv4) timestamp(ts) partition by DAY");

            execute("insert into src_vc values (0, '172.16.0.1'), (1000, '8.8.8.8')");
            execute("insert into dst_ipv4_2 select * from src_vc");

            assertSql("""
                    ts\tip
                    1970-01-01T00:00:00.000000Z\t172.16.0.1
                    1970-01-01T00:00:00.001000Z\t8.8.8.8
                    """, "dst_ipv4_2");
        });
    }

    @Test
    public void testFuzzLong128Column() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            execute("create table src (ts timestamp, l long128) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long128) timestamp(ts) partition by DAY");

            // Long128 can only be inserted via ILP or API, test with nulls
            execute("insert into src values (0, null), (1000, null)");
            execute("insert into dst select * from src");

            assertSql("count\n2\n", "select count(*) from dst");
        });
    }

    @Test
    public void testFuzzLong256Conversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // Long256 copy
            execute("create table src (ts timestamp, l long256) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l long256) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20'), (1000, null)");
            execute("insert into dst select * from src");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src order by ts", "dst order by ts", LOG);

            // Varchar to Long256
            execute("create table src_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst_l256 (ts timestamp, l long256) timestamp(ts) partition by DAY");

            execute("insert into src_vc values (0, '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'), (1000, null)");
            execute("insert into dst_l256 select * from src_vc");

            assertSql("count\n2\n", "select count(*) from dst_l256");
        });
    }

    @Test
    public void testFuzzNullHandlingLoopingVsSingleMethod() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzNullHandling(rnd);
    }

    @Test
    public void testFuzzSymbolConversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // Symbol to String
            execute("create table src_sym (ts timestamp, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst_str (ts timestamp, s string) timestamp(ts) partition by DAY");

            execute("insert into src_sym values (0, 'apple'), (1000, 'banana'), (2000, null)");
            execute("insert into dst_str select * from src_sym");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\tapple
                    1970-01-01T00:00:00.001000Z\tbanana
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst_str");

            // Symbol to Varchar
            execute("create table dst_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("insert into dst_vc select * from src_sym");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\tapple
                    1970-01-01T00:00:00.001000Z\tbanana
                    1970-01-01T00:00:00.002000Z\t
                    """, "dst_vc");

            // String to Symbol
            execute("create table src_str2 (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_sym (ts timestamp, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src_str2 values (0, 'foo'), (1000, 'bar')");
            execute("insert into dst_sym select * from src_str2");

            assertSql("""
                    ts\tsym
                    1970-01-01T00:00:00.000000Z\tfoo
                    1970-01-01T00:00:00.001000Z\tbar
                    """, "dst_sym");
        });
    }

    @Test
    public void testFuzzTypeConversionsLoopingVsChunked() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzTypeConversions(rnd, RecordToRowCopierUtils.COPIER_TYPE_CHUNKED);
    }

    @Test
    public void testFuzzTypeConversionsLoopingVsSingleMethod() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzzTypeConversions(rnd, RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD);
    }

    @Test
    public void testFuzzUuidConversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by DAY");
            execute("create table dst_str (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '550e8400-e29b-41d4-a716-446655440000'), (1000, null)");
            execute("insert into dst_str select * from src");
            execute("insert into dst_vc select * from src");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\t550e8400-e29b-41d4-a716-446655440000
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst_str");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\t550e8400-e29b-41d4-a716-446655440000
                    1970-01-01T00:00:00.001000Z\t
                    """, "dst_vc");
        });
    }

    @Test
    public void testFuzzVarcharToNumericConversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            execute("create table src (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst_byte (ts timestamp, b byte) timestamp(ts) partition by DAY");
            execute("create table dst_short (ts timestamp, s short) timestamp(ts) partition by DAY");
            execute("create table dst_int (ts timestamp, i int) timestamp(ts) partition by DAY");
            execute("create table dst_long (ts timestamp, l long) timestamp(ts) partition by DAY");
            execute("create table dst_float (ts timestamp, f float) timestamp(ts) partition by DAY");
            execute("create table dst_double (ts timestamp, d double) timestamp(ts) partition by DAY");

            execute("insert into src values (0, '42'), (1000, '-17'), (2000, '100')");

            execute("insert into dst_byte select * from src");
            execute("insert into dst_short select * from src");
            execute("insert into dst_int select * from src");
            execute("insert into dst_long select * from src");
            execute("insert into dst_float select * from src");
            execute("insert into dst_double select * from src");

            assertSql("b\n42\n-17\n100\n", "select b from dst_byte order by ts");
            assertSql("s\n42\n-17\n100\n", "select s from dst_short order by ts");
            assertSql("i\n42\n-17\n100\n", "select i from dst_int order by ts");
            assertSql("l\n42\n-17\n100\n", "select l from dst_long order by ts");
            assertSql("f\n42.0\n-17.0\n100.0\n", "select f from dst_float order by ts");
            assertSql("d\n42.0\n-17.0\n100.0\n", "select d from dst_double order by ts");
        });
    }

    private static int randomCopierType(Rnd rnd) {
        return COPIER_TYPES[rnd.nextInt(COPIER_TYPES.length)];
    }

    private void setCopierType(int copierType) {
        node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
    }

    private void testFuzzAllTypes(Rnd rnd, int copierType1, int copierType2) throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10 + rnd.nextInt(90); // 10-100 rows

            // Create source table with all supported types
            execute("create table src (" +
                    "ts timestamp, " +
                    "col_boolean boolean, " +
                    "col_byte byte, " +
                    "col_short short, " +
                    "col_char char, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_date date, " +
                    "col_float float, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_long256 long256, " +
                    "col_geobyte geohash(4b), " +
                    "col_geoshort geohash(12b), " +
                    "col_geoint geohash(24b), " +
                    "col_geolong geohash(44b), " +
                    "col_binary binary, " +
                    "col_uuid uuid, " +
                    "col_ipv4 ipv4, " +
                    "col_varchar varchar" +
                    ") timestamp(ts) partition by DAY");

            // Insert random data
            execute("insert into src select " +
                    "x::timestamp, " +
                    "rnd_boolean(), " +
                    "rnd_byte(), " +
                    "rnd_short(), " +
                    "rnd_char(), " +
                    "rnd_int(), " +
                    "rnd_long(), " +
                    "rnd_date(to_date('2020', 'yyyy'), to_date('2025', 'yyyy'), 0), " +
                    "rnd_float(), " +
                    "rnd_double(), " +
                    "rnd_str(5, 10, 2), " +
                    "rnd_symbol('A','B','C','D','E'), " +
                    "rnd_long256(), " +
                    "rnd_geohash(4), " +
                    "rnd_geohash(12), " +
                    "rnd_geohash(24), " +
                    "rnd_geohash(44), " +
                    "rnd_bin(10, 20, 2), " +
                    "rnd_uuid4(), " +
                    "rnd_ipv4(), " +
                    "rnd_varchar(5, 10, 2) " +
                    "from long_sequence(" + rowCount + ")");

            // Create destination tables
            execute("create table dst1 (" +
                    "ts timestamp, " +
                    "col_boolean boolean, " +
                    "col_byte byte, " +
                    "col_short short, " +
                    "col_char char, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_date date, " +
                    "col_float float, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_long256 long256, " +
                    "col_geobyte geohash(4b), " +
                    "col_geoshort geohash(12b), " +
                    "col_geoint geohash(24b), " +
                    "col_geolong geohash(44b), " +
                    "col_binary binary, " +
                    "col_uuid uuid, " +
                    "col_ipv4 ipv4, " +
                    "col_varchar varchar" +
                    ") timestamp(ts) partition by DAY");

            execute("create table dst2 (" +
                    "ts timestamp, " +
                    "col_boolean boolean, " +
                    "col_byte byte, " +
                    "col_short short, " +
                    "col_char char, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_date date, " +
                    "col_float float, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_long256 long256, " +
                    "col_geobyte geohash(4b), " +
                    "col_geoshort geohash(12b), " +
                    "col_geoint geohash(24b), " +
                    "col_geolong geohash(44b), " +
                    "col_binary binary, " +
                    "col_uuid uuid, " +
                    "col_ipv4 ipv4, " +
                    "col_varchar varchar" +
                    ") timestamp(ts) partition by DAY");

            // Copy using first copier type
            setCopierType(copierType1);
            execute("insert into dst1 select * from src");

            // Copy using second copier type
            setCopierType(copierType2);
            execute("insert into dst2 select * from src");

            // Compare results
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1 order by ts", "dst2 order by ts", LOG);
        });
    }

    private void testFuzzNullHandling(Rnd rnd) throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 20 + rnd.nextInt(30);

            // Create table with high null rate
            execute("create table src_nulls (" +
                    "ts timestamp, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_uuid uuid, " +
                    "col_binary binary" +
                    ") timestamp(ts) partition by DAY");

            execute("create table dst1_nulls (" +
                    "ts timestamp, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_uuid uuid, " +
                    "col_binary binary" +
                    ") timestamp(ts) partition by DAY");

            execute("create table dst2_nulls (" +
                    "ts timestamp, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_uuid uuid, " +
                    "col_binary binary" +
                    ") timestamp(ts) partition by DAY");

            // Insert with 50% nulls
            execute("insert into src_nulls select " +
                    "x::timestamp, " +
                    "case when x % 2 = 0 then null else rnd_int() end, " +
                    "case when x % 3 = 0 then null else rnd_long() end, " +
                    "case when x % 2 = 1 then null else rnd_double() end, " +
                    "rnd_str(3, 8, 5), " +  // 5 = high null rate
                    "rnd_symbol(null, 'A', 'B'), " +
                    "case when x % 4 = 0 then null else rnd_uuid4() end, " +
                    "rnd_bin(5, 10, 5) " +
                    "from long_sequence(" + rowCount + ")");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_LOOPING);
            execute("insert into dst1_nulls select * from src_nulls");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD);
            execute("insert into dst2_nulls select * from src_nulls");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1_nulls order by ts", "dst2_nulls order by ts", LOG);
        });
    }

    private void testFuzzTypeConversions(Rnd rnd, int copierType2) throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10 + rnd.nextInt(40); // 10-50 rows

            // Test numeric widening conversions: byte -> short -> int -> long -> float -> double
            execute("create table src_numeric (ts timestamp, b byte, s short, i int, l long) timestamp(ts) partition by DAY");
            execute("create table dst1_numeric (ts timestamp, b long, s long, i long, l double) timestamp(ts) partition by DAY");
            execute("create table dst2_numeric (ts timestamp, b long, s long, i long, l double) timestamp(ts) partition by DAY");

            execute("insert into src_numeric select x::timestamp, rnd_byte(), rnd_short(), rnd_int(), rnd_long() from long_sequence(" + rowCount + ")");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_LOOPING);
            execute("insert into dst1_numeric select * from src_numeric");

            setCopierType(copierType2);
            execute("insert into dst2_numeric select * from src_numeric");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1_numeric order by ts", "dst2_numeric order by ts", LOG);

            // Test string/varchar/symbol conversions
            execute("create table src_text (ts timestamp, str string, vc varchar, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst1_text (ts timestamp, str varchar, vc string, sym varchar) timestamp(ts) partition by DAY");
            execute("create table dst2_text (ts timestamp, str varchar, vc string, sym varchar) timestamp(ts) partition by DAY");

            execute("insert into src_text select x::timestamp, rnd_str(3,8,1), rnd_varchar(3,8,1), rnd_symbol('X','Y','Z') from long_sequence(" + rowCount + ")");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_LOOPING);
            execute("insert into dst1_text select * from src_text");

            setCopierType(copierType2);
            execute("insert into dst2_text select * from src_text");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1_text order by ts", "dst2_text order by ts", LOG);

            // Test geohash same-size copy
            execute("create table src_geo (ts timestamp, g4 geohash(4b), g8 geohash(8b)) timestamp(ts) partition by DAY");
            execute("create table dst1_geo (ts timestamp, g4 geohash(4b), g8 geohash(8b)) timestamp(ts) partition by DAY");
            execute("create table dst2_geo (ts timestamp, g4 geohash(4b), g8 geohash(8b)) timestamp(ts) partition by DAY");

            execute("insert into src_geo select x::timestamp, rnd_geohash(4), rnd_geohash(8) from long_sequence(" + rowCount + ")");

            setCopierType(RecordToRowCopierUtils.COPIER_TYPE_LOOPING);
            execute("insert into dst1_geo select * from src_geo");

            setCopierType(copierType2);
            execute("insert into dst2_geo select * from src_geo");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst1_geo order by ts", "dst2_geo order by ts", LOG);
        });
    }
}
