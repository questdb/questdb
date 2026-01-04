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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class RecordToRowCopierUtilsTest extends AbstractCairoTest {

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
    public void testCopierByteToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 123), getLongAsserter(123000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierByteToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(4, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 99), new RowAsserter());
    }

    @Test
    public void testCopierDecimal8ToDecimal32() {
        // Converting 1.2 from a Decimal8 with a scale of 1 (1.2) to a Decimal32 with a scale of 3 (1.200)
        int fromType = ColumnType.getDecimalType(2, 1);
        Assert.assertEquals("Unexpected from type", ColumnType.DECIMAL8, ColumnType.tagOf(fromType));
        int toType = ColumnType.getDecimalType(8, 3);
        Assert.assertEquals("Unexpected to type", ColumnType.DECIMAL32, ColumnType.tagOf(toType));

        RecordToRowCopier copier = generateCopier(fromType, toType);

        Record rec = new Record() {
            @Override
            public byte getDecimal8(int col) {
                return 12;
            }
        };

        copier.copy(sqlExecutionContext, rec, getIntAsserter());
    }

    @Test
    public void testCopierDecimalCases() {
        Decimal256 d = new Decimal256();
        Decimal256 c = new Decimal256();
        Decimal256 dnull = new Decimal256();
        BytecodeAssembler asm = new BytecodeAssembler();
        dnull.ofNull();
        boolean[] isNull = new boolean[]{false, true};
        int[] fromPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        int[] toPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        for (int fromPrecision : fromPrecisions) {
            for (int toPrecision : toPrecisions) {
                for (boolean nulled : isNull) {
                    if (nulled) {
                        testDecimalCast(asm, fromPrecision, toPrecision, 0, dnull, true);
                        continue;
                    }

                    for (int fromScale = 1; fromScale <= fromPrecision; fromScale <<= 1) {
                        for (int toScale = 1; toScale <= toPrecision; toScale <<= 1) {
                            boolean fit;
                            generateValue(d, fromPrecision, fromScale);
                            try {
                                c.copyFrom(d);
                                c.rescale(toScale);
                                fit = c.comparePrecision(toPrecision);
                            } catch (NumericException ignored) {
                                fit = false;
                            }
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                            d.negate();
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCopierIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 123456), getLongAsserter(123456000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierIntToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 999999), new RowAsserter());
    }

    @Test
    public void testCopierLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 123456L), getLongAsserter(123456000L));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierLongToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 999999L), new RowAsserter());
    }

    @Test
    public void testCopierNullIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, Numbers.INT_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierNullLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, Numbers.LONG_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierShortToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 1234), getLongAsserter(1234000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierShortToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(5, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 9999), new RowAsserter());
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

            // Varchar expression (cast) to Long256 - tests non-DirectUtf8Sequence path
            execute("create table src_str (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_l256_cast (ts timestamp, l long256) timestamp(ts) partition by DAY");

            execute("insert into src_str values (0, '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'), (1000, null)");
            execute("insert into dst_l256_cast select ts, s::varchar from src_str");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "dst_l256 order by ts", "dst_l256_cast order by ts", LOG);

            // Negative test: non-ASCII varchar to Long256 should fail
            execute("create table src_non_ascii (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_l256_fail (ts timestamp, l long256) timestamp(ts) partition by DAY");
            execute("insert into src_non_ascii values (0, '0xкириллица')");

            assertException(
                    "insert into dst_l256_fail select ts, s::varchar from src_non_ascii",
                    0,
                    "inconvertible value"
            );
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

    @Test
    public void testFuzzVarcharStringConversions() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int copierType = randomCopierType(rnd);
            setCopierType(copierType);

            // VARCHAR column to STRING column
            execute("create table src_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("create table dst_str (ts timestamp, s string) timestamp(ts) partition by DAY");

            execute("insert into src_vc values (0, 'hello'), (1000, 'world'), (2000, null), (3000, 'über'), (4000, '日本語')");
            execute("insert into dst_str select * from src_vc");

            assertSql("""
                    ts\ts
                    1970-01-01T00:00:00.000000Z\thello
                    1970-01-01T00:00:00.001000Z\tworld
                    1970-01-01T00:00:00.002000Z\t
                    1970-01-01T00:00:00.003000Z\tüber
                    1970-01-01T00:00:00.004000Z\t日本語
                    """, "dst_str order by ts");

            // STRING column to VARCHAR column
            execute("create table src_str (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("create table dst_vc (ts timestamp, v varchar) timestamp(ts) partition by DAY");

            execute("insert into src_str values (0, 'foo'), (1000, 'bar'), (2000, null), (3000, 'café'), (4000, '日本語'), (5000, 'Привет'), (6000, '🎉emoji🚀')");
            execute("insert into dst_vc select * from src_str");

            assertSql("""
                    ts\tv
                    1970-01-01T00:00:00.000000Z\tfoo
                    1970-01-01T00:00:00.001000Z\tbar
                    1970-01-01T00:00:00.002000Z\t
                    1970-01-01T00:00:00.003000Z\tcafé
                    1970-01-01T00:00:00.004000Z\t日本語
                    1970-01-01T00:00:00.005000Z\tПривет
                    1970-01-01T00:00:00.006000Z\t🎉emoji🚀
                    """, "dst_vc order by ts");

            // STRING expression (cast) to VARCHAR column with non-ASCII
            execute("create table dst_vc2 (ts timestamp, v varchar) timestamp(ts) partition by DAY");
            execute("insert into dst_vc2 select ts, v::string from src_vc");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src_vc order by ts", "dst_vc2 order by ts", LOG);

            // VARCHAR expression (cast) to STRING column - tests function result path
            // This exercises the transferVarcharToStrCol helper with non-DirectUtf8Sequence values
            execute("create table dst_str2 (ts timestamp, s string) timestamp(ts) partition by DAY");
            execute("insert into dst_str2 select ts, s::varchar from src_str");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src_str order by ts", "dst_str2 order by ts", LOG);
        });
    }

    private static @NotNull Record getLongRecord(int fromType, long value) {
        return new Record() {
            @Override
            public byte getByte(int col) {
                Assert.assertEquals(ColumnType.BYTE, fromType);
                return (byte) value;
            }

            @Override
            public int getInt(int col) {
                Assert.assertEquals(ColumnType.INT, fromType);
                return (int) value;
            }

            @Override
            public long getLong(int col) {
                Assert.assertEquals(ColumnType.LONG, fromType);
                return value;
            }

            @Override
            public short getShort(int col) {
                Assert.assertEquals(ColumnType.SHORT, fromType);
                return (short) value;
            }
        };
    }

    private static int randomCopierType(Rnd rnd) {
        return COPIER_TYPES[rnd.nextInt(COPIER_TYPES.length)];
    }

    private RecordToRowCopier generateCopier(int fromType, int toType) {
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        return RecordToRowCopierUtils.generateCopier(new BytecodeAssembler(), from, to, mapping, configuration);
    }

    private void generateValue(Decimal256 d, int precision, int scale) {
        String maxValue = "98765432109876543210987654321098765432109876543210987654321098765432109876543210";
        BigDecimal value = new BigDecimal(maxValue.substring(0, precision));
        Decimal256 v = Decimal256.fromBigDecimal(value);
        d.of(v.getHh(), v.getHl(), v.getLh(), v.getLl(), scale);
    }

    private TableWriter.Row getIntAsserter() {
        return new RowAsserter() {
            @Override
            public void putInt(int col, int value) {
                Assert.assertEquals(1200, value);
            }
        };
    }

    private TableWriter.Row getLongAsserter(long expected) {
        return new RowAsserter() {
            @Override
            public void putLong(int col, long value) {
                Assert.assertEquals(expected, value);
            }
        };
    }

    private void setCopierType(int copierType) {
        node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
    }

    private void testDecimalCast(BytecodeAssembler asm, int fromPrecision, int toPrecision, int toScale, Decimal256 value, boolean fitInTargetType) {
        int fromType = ColumnType.getDecimalType(fromPrecision, value.getScale());
        int toType = ColumnType.getDecimalType(toPrecision, toScale);
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, from, to, mapping, configuration);

        Decimal256 expectedValue;
        if (fitInTargetType) {
            expectedValue = new Decimal256();
            expectedValue.copyFrom(value);
            expectedValue.rescale(toScale);
        } else {
            expectedValue = null;
        }

        Record rec = DecimalUtilTest.getDecimalRecord(fromType, value);
        TableWriter.Row row = DecimalUtilTest.getRowAsserter(toType, -1, expectedValue);
        try {
            copier.copy(sqlExecutionContext, rec, row);
            if (!fitInTargetType) {
                Assert.fail(String.format("Expected cast to fail from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for %s",
                        ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                        ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                        value));
            }
        } catch (ImplicitCastException e) {
            if (fitInTargetType) {
                throw e;
            }
        } catch (AssertionError e) {
            System.err.printf("Cast failed from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for '%s'\n",
                    ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                    ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                    value);
            throw e;
        }
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
