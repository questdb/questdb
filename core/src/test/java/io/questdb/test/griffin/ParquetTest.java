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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Miscellaneous tests for tables with partitions in Parquet format.
 */
public class ParquetTest extends AbstractCairoTest {
    private static final String ARRAY_1D = "ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]";
    private static final String ARRAY_2D = "ARRAY[" +
            "  [1.0, 2.0, 3.0]," +
            "  [4.0, 5.0, 6.0]," +
            "  [7.0, 8.0, 9.0]," +
            "  [10.0, 11.0, 12.0]" +
            "]";
    private static final String ARRAY_3D = "ARRAY[" +
            "  [" +
            "    [1.0, 2.0, 3.0]," +
            "    [4.0, 5.0, 6.0]," +
            "    [7.0, 8.0, 9.0]" +
            "  ]," +
            "  [" +
            "    [10.0, 11.0, 12.0]," +
            "    [13.0, 14.0, 15.0]," +
            "    [16.0, 17.0, 18.0]" +
            "  ]," +
            "  [" +
            "    [19.0, 20.0, 21.0]," +
            "    [22.0, 23.0, 24.0]," +
            "    [25.0, 26.0, 27.0]" +
            "  ]" +
            "]";
    private static final String ARRAY_4D = "ARRAY[" +
            "  [" +
            "    [" +
            "      [1.0, 2.0]," +
            "      [3.0, 4.0]" +
            "    ]," +
            "    [" +
            "      [5.0, 6.0]," +
            "      [7.0, 8.0]" +
            "    ]" +
            "  ]," +
            "  [" +
            "    [" +
            "      [9.0, 10.0]," +
            "      [11.0, 12.0]" +
            "    ]," +
            "    [" +
            "      [13.0, 14.0]," +
            "      [15.0, 16.0]" +
            "    ]" +
            "  ]" +
            "]";
    private static final String ARRAY_5D = "ARRAY[" +
            "  [" +
            "    [" +
            "      [" +
            "        [1.0, 2.0]," +
            "        [3.0, 4.0]" +
            "      ]," +
            "      [" +
            "        [5.0, 6.0]," +
            "        [7.0, 8.0]" +
            "      ]" +
            "    ]," +
            "    [" +
            "      [" +
            "        [9.0, 10.0]," +
            "        [11.0, 12.0]" +
            "      ]," +
            "      [" +
            "        [13.0, 14.0]," +
            "        [15.0, 16.0]" +
            "      ]" +
            "    ]" +
            "  ]," +
            "  [" +
            "    [" +
            "      [" +
            "        [17.0, 18.0]," +
            "        [19.0, 20.0]" +
            "      ]," +
            "      [" +
            "        [21.0, 22.0]," +
            "        [23.0, 24.0]" +
            "      ]" +
            "    ]," +
            "    [" +
            "      [" +
            "        [25.0, 26.0]," +
            "        [27.0, 28.0]" +
            "      ]," +
            "      [" +
            "        [29.0, 30.0]," +
            "        [31.0, 32.0]" +
            "      ]" +
            "    ]" +
            "  ]" +
            "]";

    @Test
    public void test1dArray() throws Exception {
        testNdArray(1, ARRAY_1D, false);
    }

    @Test
    public void test1dArray_rawArrayEncoding() throws Exception {
        testNdArray(1, ARRAY_1D, true);
    }

    @Test
    public void test2dArray() throws Exception {
        testNdArray(2, ARRAY_2D, false);
    }

    @Test
    public void test2dArray_rawArrayEncoding() throws Exception {
        testNdArray(2, ARRAY_2D, true);
    }

    @Test
    public void test3dArray() throws Exception {
        testNdArray(3, ARRAY_3D, false);
    }

    @Test
    public void test3dArray_rawArrayEncoding() throws Exception {
        testNdArray(3, ARRAY_3D, true);
    }

    @Test
    public void test4dArray() throws Exception {
        testNdArray(4, ARRAY_4D, false);
    }

    @Test
    public void test4dArray_rawArrayEncoding() throws Exception {
        testNdArray(4, ARRAY_4D, true);
    }

    @Test
    public void test5dArray() throws Exception {
        testNdArray(5, ARRAY_5D, false);
    }

    @Test
    public void test5dArray_rawArrayEncoding() throws Exception {
        testNdArray(5, ARRAY_5D, true);
    }

    @Test
    public void testArrayColTops() throws Exception {
        testArrayColTops(false);
    }

    @Test
    public void testArrayColTops_rawArrayEncoding() throws Exception {
        testArrayColTops(true);
    }

    @Test
    public void testArraySmallDataPages() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select array[42] arr, timestamp_sequence(0,1000000) as ts
                              from long_sequence(10000)
                            ) timestamp(ts) partition by day;"""
            );
            // create new active partition
            execute("insert into x values (null, '2000-01-01')");
            execute("alter table x convert partition to parquet where ts >= 0");

            assertQuery(
                    """
                            count
                            10000
                            """,
                    "select count() from x where arr[1] = 42",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id long, ts timestamp) timestamp(ts) partition by month;");
            execute("insert into x values(1, '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values(2, '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values(3, '2024-06-12T00:00:00.000000Z');");
            execute("insert into x values(4, '2024-06-12T00:00:01.000000Z');");
            execute("insert into x values(5, '2024-06-15T00:00:00.000000Z');");
            execute("insert into x values(6, '2024-06-12T00:00:02.000000Z');");

            execute("alter table x add column a int;");
            execute("insert into x values(7, '2024-06-10T00:00:00.000000Z', 1);");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(
                    """
                            id\tts\ta
                            1\t2024-06-10T00:00:00.000000Z\tnull
                            7\t2024-06-10T00:00:00.000000Z\t1
                            2\t2024-06-11T00:00:00.000000Z\tnull
                            3\t2024-06-12T00:00:00.000000Z\tnull
                            4\t2024-06-12T00:00:01.000000Z\tnull
                            6\t2024-06-12T00:00:02.000000Z\tnull
                            5\t2024-06-15T00:00:00.000000Z\tnull
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testConvertToNativeFailure() throws Exception {
        // Verify that we aren't closing garbage fds when parquetDecoder.of() fail.
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:00.000000Z');");

            execute("alter table x convert partition to parquet where ts >= 0");

            long rss = Unsafe.getRssMemUsed();
            // Leave enough room for columnFdAndDataSize list only
            Unsafe.setRssMemLimit(rss + 48);
            try {
                execute("alter table x convert partition to native where ts >= 0");
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            } finally {
                Unsafe.setRssMemLimit(0);
            }
        });
    }

    @Test
    public void testDecimalAllSizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "id long, " +
                    "ts timestamp, " +
                    "dec8 decimal(2,1), " +
                    "dec16 decimal(4,2), " +
                    "dec32 decimal(9,4), " +
                    "dec64 decimal(18,6), " +
                    "dec128 decimal(38,10), " +
                    "dec256 decimal(76,20)" +
                    ") timestamp(ts) partition by day;");

            execute("insert into x values(" +
                    "1, '2024-06-10T00:00:00.000000Z', " +
                    "1.2::decimal(2,1), " +
                    "12.34::decimal(4,2), " +
                    "12345.6789::decimal(9,4), " +
                    "123456789012.345678::decimal(18,6), " +
                    "1234567890123456789012345678.9012345678::decimal(38,10), " +
                    "12345678901234567890123456789012345678901234567890123456.78901234567890123456::decimal(76,20)" +
                    ");");

            execute("insert into x values(" +
                    "2, '2024-06-11T00:00:00.000000Z', " +
                    "-1.2::decimal(2,1), " +
                    "-12.34::decimal(4,2), " +
                    "-12345.6789::decimal(9,4), " +
                    "-123456789012.345678::decimal(18,6), " +
                    "-1234567890123456789012345678.9012345678::decimal(38,10), " +
                    "-12345678901234567890123456789012345678901234567890123456.78901234567890123456::decimal(76,20)" +
                    ");");

            execute("insert into x values(" +
                    "3, '2024-06-12T00:00:00.000000Z', " +
                    "0::decimal(2,1), " +
                    "0::decimal(4,2), " +
                    "0::decimal(9,4), " +
                    "0::decimal(18,6), " +
                    "0::decimal(38,10), " +
                    "0::decimal(76,20)" +
                    ");");

            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts\tdec8\tdec16\tdec32\tdec64\tdec128\tdec256
                            1\t2024-06-10T00:00:00.000000Z\t1.2\t12.34\t12345.6789\t123456789012.345678\t1234567890123456789012345678.9012345678\t12345678901234567890123456789012345678901234567890123456.78901234567890123456
                            2\t2024-06-11T00:00:00.000000Z\t-1.2\t-12.34\t-12345.6789\t-123456789012.345678\t-1234567890123456789012345678.9012345678\t-12345678901234567890123456789012345678901234567890123456.78901234567890123456
                            3\t2024-06-12T00:00:00.000000Z\t0.0\t0.00\t0.0000\t0.000000\t0.0000000000\t0.00000000000000000000
                            """,
                    "x order by id"
            );
        });
    }

    @Test
    public void testDecimalColTops() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 5;
        final int initialRows = 20;
        final int additionalRows = 50;

        for (int iter = 0; iter < iterations; iter++) {
            assertMemoryLeak(() -> {
                // Generate random precision/scale for each decimal type
                // DECIMAL8: precision 1-2, DECIMAL16: 3-4, DECIMAL32: 5-9,
                // DECIMAL64: 10-18, DECIMAL128: 19-38, DECIMAL256: 39-76
                int p8 = 1 + rnd.nextInt(2);
                int s8 = rnd.nextInt(p8 + 1);
                int p16 = 3 + rnd.nextInt(2);
                int s16 = rnd.nextInt(p16 + 1);
                int p32 = 5 + rnd.nextInt(5);
                int s32 = rnd.nextInt(p32 + 1);
                int p64 = 10 + rnd.nextInt(9);
                int s64 = rnd.nextInt(p64 + 1);
                int p128 = 19 + rnd.nextInt(20);
                int s128 = rnd.nextInt(p128 + 1);
                int p256 = 39 + rnd.nextInt(38);
                int s256 = rnd.nextInt(p256 + 1);

                // Create table with initial rows (no decimal columns yet)
                execute("create table x (ts timestamp) timestamp(ts) partition by month;");
                execute("insert into x " +
                        "select timestamp_sequence('2024-01-01', 100000000) as ts " +
                        "from long_sequence(" + initialRows + ")");

                // Add all 6 decimal column types with random precision/scale (these will have col-tops)
                execute("alter table x add column dec8 decimal(" + p8 + "," + s8 + ");");
                execute("alter table x add column dec16 decimal(" + p16 + "," + s16 + ");");
                execute("alter table x add column dec32 decimal(" + p32 + "," + s32 + ");");
                execute("alter table x add column dec64 decimal(" + p64 + "," + s64 + ");");
                execute("alter table x add column dec128 decimal(" + p128 + "," + s128 + ");");
                execute("alter table x add column dec256 decimal(" + p256 + "," + s256 + ");");

                // Insert additional rows with random decimal values
                execute("insert into x " +
                        "select " +
                        "timestamp_sequence('2024-02-01', 100000000) as ts, " +
                        "rnd_decimal(" + p8 + ", " + s8 + ", 5) as dec8, " +
                        "rnd_decimal(" + p16 + ", " + s16 + ", 5) as dec16, " +
                        "rnd_decimal(" + p32 + ", " + s32 + ", 5) as dec32, " +
                        "rnd_decimal(" + p64 + ", " + s64 + ", 5) as dec64, " +
                        "rnd_decimal(" + p128 + ", " + s128 + ", 5) as dec128, " +
                        "rnd_decimal(" + p256 + ", " + s256 + ", 5) as dec256 " +
                        "from long_sequence(" + additionalRows + ")");

                // Capture expected data before parquet conversion
                execute("create table expected as (select * from x)");

                // Convert to parquet
                execute("alter table x convert partition to parquet where ts >= 0");

                // Verify row count
                assertSql(
                        "cnt\n" + (initialRows + additionalRows) + "\n",
                        "select count(*) as cnt from x"
                );

                // Verify initial rows have null decimals (col-tops)
                assertSql(
                        "null_count\n" + initialRows + "\n",
                        "select count(*) as null_count from x where dec8 is null and dec16 is null and dec32 is null and dec64 is null and dec128 is null and dec256 is null"
                );

                // Verify data matches expected
                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                // Round-trip: convert back to native and verify again
                execute("alter table x convert partition to native where ts >= 0");
                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                execute("drop table x");
                execute("drop table expected");
            });
        }
    }

    @Test
    public void testDecimalFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 5;
        final int rowCount = 200;

        for (int iter = 0; iter < iterations; iter++) {
            assertMemoryLeak(() -> {
                // Generate random precision/scale for each decimal type
                int p8 = 1 + rnd.nextInt(2);
                int s8 = rnd.nextInt(p8 + 1);
                int p16 = 3 + rnd.nextInt(2);
                int s16 = rnd.nextInt(p16 + 1);
                int p32 = 5 + rnd.nextInt(5);
                int s32 = rnd.nextInt(p32 + 1);
                int p64 = 10 + rnd.nextInt(9);
                int s64 = rnd.nextInt(p64 + 1);
                int p128 = 19 + rnd.nextInt(20);
                int s128 = rnd.nextInt(p128 + 1);
                int p256 = 39 + rnd.nextInt(38);
                int s256 = rnd.nextInt(p256 + 1);

                // Create table with all decimal sizes using random precision/scale
                execute("create table x (" +
                        "ts timestamp, " +
                        "dec8 decimal(" + p8 + "," + s8 + "), " +
                        "dec16 decimal(" + p16 + "," + s16 + "), " +
                        "dec32 decimal(" + p32 + "," + s32 + "), " +
                        "dec64 decimal(" + p64 + "," + s64 + "), " +
                        "dec128 decimal(" + p128 + "," + s128 + "), " +
                        "dec256 decimal(" + p256 + "," + s256 + ")" +
                        ") timestamp(ts) partition by day;");

                // Insert random data using rnd_decimal function
                execute("insert into x " +
                        "select " +
                        "timestamp_sequence('2024-01-01', 100000000) as ts, " +
                        "rnd_decimal(" + p8 + ", " + s8 + ", 10) as dec8, " +
                        "rnd_decimal(" + p16 + ", " + s16 + ", 10) as dec16, " +
                        "rnd_decimal(" + p32 + ", " + s32 + ", 10) as dec32, " +
                        "rnd_decimal(" + p64 + ", " + s64 + ", 10) as dec64, " +
                        "rnd_decimal(" + p128 + ", " + s128 + ", 10) as dec128, " +
                        "rnd_decimal(" + p256 + ", " + s256 + ", 10) as dec256 " +
                        "from long_sequence(" + rowCount + ")");

                // Capture data before parquet conversion
                execute("create table expected as (select * from x)");

                // Convert to parquet
                execute("alter table x convert partition to parquet where ts >= 0");

                // Verify data matches after parquet conversion
                assertSql(
                        "cnt\n" + rowCount + "\n",
                        "select count(*) as cnt from x"
                );

                // Compare each row - join and check for differences
                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                // Convert back to native and verify again
                execute("alter table x convert partition to native where ts >= 0");
                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                execute("drop table x");
                execute("drop table expected");
            });
        }
    }

    @Test
    public void testDecimalFuzzLargeValues() throws Exception {
        // Test with larger values and edge cases for Decimal128 and Decimal256
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 5;
        final int rowCount = 100;

        for (int iter = 0; iter < iterations; iter++) {
            assertMemoryLeak(() -> {
                // Generate random precision/scale for Decimal128 and Decimal256
                int p128a = 19 + rnd.nextInt(10);  // smaller Decimal128
                int s128a = rnd.nextInt(p128a + 1);
                int p128b = 29 + rnd.nextInt(10);  // larger Decimal128
                int s128b = rnd.nextInt(p128b + 1);
                int p256a = 39 + rnd.nextInt(10);  // smaller Decimal256
                int s256a = rnd.nextInt(p256a + 1);
                int p256b = 60 + rnd.nextInt(17);  // larger Decimal256
                int s256b = rnd.nextInt(p256b + 1);

                // Create table focusing on large decimal types
                execute("create table x (" +
                        "ts timestamp, " +
                        "dec128_small decimal(" + p128a + "," + s128a + "), " +
                        "dec128_large decimal(" + p128b + "," + s128b + "), " +
                        "dec256_small decimal(" + p256a + "," + s256a + "), " +
                        "dec256_large decimal(" + p256b + "," + s256b + ")" +
                        ") timestamp(ts) partition by day;");

                // Insert random data
                execute("insert into x " +
                        "select " +
                        "timestamp_sequence('2024-01-01', 100000000) as ts, " +
                        "rnd_decimal(" + p128a + ", " + s128a + ", 5) as dec128_small, " +
                        "rnd_decimal(" + p128b + ", " + s128b + ", 5) as dec128_large, " +
                        "rnd_decimal(" + p256a + ", " + s256a + ", 5) as dec256_small, " +
                        "rnd_decimal(" + p256b + ", " + s256b + ", 5) as dec256_large " +
                        "from long_sequence(" + rowCount + ")");

                // Capture data before parquet conversion
                execute("create table expected as (select * from x)");

                // Convert to parquet
                execute("alter table x convert partition to parquet where ts >= 0");

                // Verify data matches
                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                // Round-trip test: convert back to native and then to parquet again
                execute("alter table x convert partition to native where ts >= 0");
                execute("alter table x convert partition to parquet where ts >= 0");

                assertSql(
                        "diff_count\n0\n",
                        "select count(*) as diff_count from (" +
                                "select * from x " +
                                "except " +
                                "select * from expected" +
                                ")"
                );

                execute("drop table x");
                execute("drop table expected");
            });
        }
    }

    @Test
    public void testDecimalRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "id long, " +
                    "ts timestamp, " +
                    "dec32 decimal(9,4), " +
                    "dec64 decimal(18,6), " +
                    "dec128 decimal(38,10)" +
                    ") timestamp(ts) partition by day;");

            execute("insert into x values(1, '2024-06-10T00:00:00.000000Z', 123.4567::decimal(9,4), 123456.789012::decimal(18,6), 12345678901234567890.1234567890::decimal(38,10));");
            execute("insert into x values(2, '2024-06-11T00:00:00.000000Z', -987.6543::decimal(9,4), -999999.999999::decimal(18,6), -98765432109876543210.9876543210::decimal(38,10));");
            execute("insert into x values(3, '2024-06-12T00:00:00.000000Z', null, null, null);");

            final String expected = """
                    id\tts\tdec32\tdec64\tdec128
                    1\t2024-06-10T00:00:00.000000Z\t123.4567\t123456.789012\t12345678901234567890.1234567890
                    2\t2024-06-11T00:00:00.000000Z\t-987.6543\t-999999.999999\t-98765432109876543210.9876543210
                    3\t2024-06-12T00:00:00.000000Z\t\t\t
                    """;

            // Convert to parquet
            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(expected, "x order by id");

            // Convert back to native
            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, "x order by id");

            // Convert to parquet again
            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(expected, "x order by id");
        });
    }

    @Test
    public void testDecimalWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "id long, " +
                    "ts timestamp, " +
                    "dec32 decimal(9,4), " +
                    "dec64 decimal(18,6)" +
                    ") timestamp(ts) partition by day;");

            execute("insert into x values(1, '2024-06-10T00:00:00.000000Z', 123.4567::decimal(9,4), 123456.789012::decimal(18,6));");
            execute("insert into x values(2, '2024-06-11T00:00:00.000000Z', null, 999.999999::decimal(18,6));");
            execute("insert into x values(3, '2024-06-12T00:00:00.000000Z', 987.6543::decimal(9,4), null);");
            execute("insert into x values(4, '2024-06-13T00:00:00.000000Z', null, null);");

            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts\tdec32\tdec64
                            1\t2024-06-10T00:00:00.000000Z\t123.4567\t123456.789012
                            2\t2024-06-11T00:00:00.000000Z\t\t999.999999
                            3\t2024-06-12T00:00:00.000000Z\t987.6543\t
                            4\t2024-06-13T00:00:00.000000Z\t\t
                            """,
                    "x order by id"
            );
        });
    }

    @Test
    public void testDedupFixedKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts, x) ;");

            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (3, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    """, "x");

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    """, "x");

            drainWalQueue();

            execute("insert into x(x,ts) values (11, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (22, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (33, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    11\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    22\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    33\t2020-01-03T00:00:00.000000Z
                    """, "x");
        });
    }

    @Test
    public void testDedupTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts) ;");

            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (3, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    """, "x");

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    """, "x");

            drainWalQueue();

            execute("insert into x(x,ts) values (11, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (22, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (33, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("""
                    x\tts
                    11\t2020-01-01T00:00:00.000000Z
                    22\t2020-01-02T00:00:00.000000Z
                    33\t2020-01-03T00:00:00.000000Z
                    """, "x");
        });
    }

    @Test
    public void testDedupVarlenKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x varchar, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts, x) ;");

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            """,
                    "x"
            );

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            """,
                    "x"
            );

            drainWalQueue();

            execute("insert into x(x,ts) values ('100000000001', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('200000000002', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('33', '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            100000000001\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            200000000002\t2020-01-02T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            33\t2020-01-03T00:00:00.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testFilterAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            3\t1970-01-01T00:33:20.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            1\t1970-01-01T00:00:00.000000Z
                            """,
                    "x where id < 4 order by id desc"
            );
        });
    }

    @Test
    public void testFilterArray() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select array[x] id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            [3.0]\t1970-01-01T00:33:20.000000Z
                            [2.0]\t1970-01-01T00:16:40.000000Z
                            [1.0]\t1970-01-01T00:00:00.000000Z
                            """,
                    "x where id[1] < 4 order by ts desc"
            );
        });
    }

    @Test
    public void testIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T00:00:01.000000Z');");
            execute("insert into x values('k2', '2024-06-15T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:02.000000Z');");

            execute("alter table x alter column id add index;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");

            final String expected = """
                    id\tts
                    k1\t2024-06-10T00:00:00.000000Z
                    k1\t2024-06-10T00:00:00.000000Z
                    k1\t2024-06-12T00:00:01.000000Z
                    """;
            final String query = "x where id = 'k1'";

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id drop index;");
            assertSql(expected, query);

            execute("alter table x alter column id add index;");
            assertSql(expected, query);
        });
    }

    // TODO(puzpuzpuz): enable when we support DDLs for parquet partitions
    @Ignore
    @Test
    public void testIndexBumpedColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");

            // bump column version
            execute("alter table x drop column id;");
            execute("alter table x add column id symbol;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:02.000000Z');");

            // bump column version one more time
            execute("alter table x alter column id add index;");
            execute("alter table x alter column id drop index;");

            execute("alter table x convert partition to parquet where ts >= 0");

            final String expected = """
                    ts\tid
                    2024-06-10T01:00:00.000000Z\tk1
                    2024-06-11T01:00:00.000000Z\tk2
                    2024-06-12T01:00:00.000000Z\tk3
                    2024-06-12T01:00:01.000000Z\tk1
                    2024-06-12T01:00:02.000000Z\tk3
                    2024-06-15T01:00:00.000000Z\tk2
                    """;
            final String query = "x";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testIndexColTopColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('2024-06-10T00:00:00.000000Z');");

            execute("alter table x add column id symbol");
            execute("insert into x values('2024-06-11T00:00:00.000000Z', 'k1');");
            execute("insert into x values('2024-06-12T00:00:00.000000Z', 'k2');");
            execute("insert into x values('2024-06-12T00:00:01.000000Z', 'k3');");
            execute("insert into x values('2024-06-15T00:00:00.000000Z', 'k3');");

            execute("alter table x alter column id add index;");
            execute("insert into x values('2024-06-10T00:00:00.000000Z', 'k1');");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(
                    """
                            ts\tid
                            2024-06-10T00:00:00.000000Z\tk1
                            2024-06-11T00:00:00.000000Z\tk1
                            """,
                    "x where id = 'k1'"
            );
        });
    }

    @Test
    public void testIndexO3Writes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T01:00:02.000000Z');");

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id add index;");

            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T00:00:00.000000Z');");

            final String expected = """
                    id\tts
                    k1\t2024-06-10T00:00:00.000000Z
                    k1\t2024-06-10T01:00:00.000000Z
                    k1\t2024-06-11T00:00:00.000000Z
                    k1\t2024-06-12T00:00:00.000000Z
                    k1\t2024-06-12T01:00:01.000000Z
                    """;
            final String query = "x where id = 'k1'";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testIndexO3WritesBumpedColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:02.000000Z');");

            // bump column version
            execute("alter table x alter column id add index;");
            execute("alter table x alter column id drop index;");

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id add index;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T00:00:00.000000Z');");

            final String expected = """
                    id\tts
                    k1\t2024-06-10T00:00:00.000000Z
                    k1\t2024-06-10T01:00:00.000000Z
                    k1\t2024-06-11T00:00:00.000000Z
                    k1\t2024-06-12T00:00:00.000000Z
                    k1\t2024-06-12T01:00:01.000000Z
                    """;
            final String query = "x where id = 'k1'";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testJitFilter() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            """,
                    "x where id < 4"
            );
        });
    }

    @Test
    public void testLimitRightFrameFormat() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id	ts
                            5	1970-01-01T01:06:40.000000Z
                            6	1970-01-01T01:23:20.000000Z
                            7	1970-01-01T01:40:00.000000Z
                            """,
                    "x where ts <= '1970-01-01T01:40:00.000000' limit -3"
            );
        });
    }

    @Test
    public void testMixedPartitionsNativeLast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts = '1970-01-01T02'");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            4\t1970-01-01T00:50:00.000000Z
                            5\t1970-01-01T01:06:40.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            8\t1970-01-01T01:56:40.000000Z
                            9\t1970-01-01T02:13:20.000000Z
                            10\t1970-01-01T02:30:00.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testMixedPartitionsParquetLast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts = '1970-01-01T01'");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            4\t1970-01-01T00:50:00.000000Z
                            5\t1970-01-01T01:06:40.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            8\t1970-01-01T01:56:40.000000Z
                            9\t1970-01-01T02:13:20.000000Z
                            10\t1970-01-01T02:30:00.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            4\t1970-01-01T00:50:00.000000Z
                            5\t1970-01-01T01:06:40.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            8\t1970-01-01T01:56:40.000000Z
                            9\t1970-01-01T02:13:20.000000Z
                            10\t1970-01-01T02:30:00.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testNonJitFilter() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            """,
                    "x where id < 4"
            );
        });
    }

    @Test
    public void testNonWildcardSelect1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            1\tts\tid\tid2\tts2
                            1\t1970-01-01T00:00:00.000000Z\t1\t1\t1970-01-01T00:00:00.000000Z
                            1\t1970-01-01T00:16:40.000000Z\t2\t2\t1970-01-01T00:16:40.000000Z
                            1\t1970-01-01T00:33:20.000000Z\t3\t3\t1970-01-01T00:33:20.000000Z
                            1\t1970-01-01T00:50:00.000000Z\t4\t4\t1970-01-01T00:50:00.000000Z
                            1\t1970-01-01T01:06:40.000000Z\t5\t5\t1970-01-01T01:06:40.000000Z
                            1\t1970-01-01T01:23:20.000000Z\t6\t6\t1970-01-01T01:23:20.000000Z
                            1\t1970-01-01T01:40:00.000000Z\t7\t7\t1970-01-01T01:40:00.000000Z
                            1\t1970-01-01T01:56:40.000000Z\t8\t8\t1970-01-01T01:56:40.000000Z
                            1\t1970-01-01T02:13:20.000000Z\t9\t9\t1970-01-01T02:13:20.000000Z
                            1\t1970-01-01T02:30:00.000000Z\t10\t10\t1970-01-01T02:30:00.000000Z
                            """,
                    "select 1, ts, id, id as id2, ts as ts2 from x"
            );
        });
    }

    @Test
    public void testNonWildcardSelect2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            ts
                            1970-01-01T00:00:00.000000Z
                            1970-01-01T00:16:40.000000Z
                            1970-01-01T00:33:20.000000Z
                            1970-01-01T00:50:00.000000Z
                            1970-01-01T01:06:40.000000Z
                            1970-01-01T01:23:20.000000Z
                            1970-01-01T01:40:00.000000Z
                            1970-01-01T01:56:40.000000Z
                            1970-01-01T02:13:20.000000Z
                            1970-01-01T02:30:00.000000Z
                            """,
                    "select ts from x"
            );
        });
    }

    @Test
    public void testO3Inserts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day;");

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            execute("alter table x convert partition to parquet list '2020-01-02';");
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            """,
                    "x"
            );

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            3\t2020-01-03T00:00:00.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testOrderBy1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // Order by single long column uses only a single record
            assertSql(
                    """
                            id\tts
                            10\t1970-01-01T02:30:00.000000Z
                            9\t1970-01-01T02:13:20.000000Z
                            8\t1970-01-01T01:56:40.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            5\t1970-01-01T01:06:40.000000Z
                            4\t1970-01-01T00:50:00.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            1\t1970-01-01T00:00:00.000000Z
                            """,
                    "x order by id desc"
            );
        });
    }

    @Test
    public void testOrderBy2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x%5 id1, x id2, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // Order by single long column uses both records
            assertSql(
                    """
                            id1\tid2\tts
                            4\t4\t1970-01-01T00:50:00.000000Z
                            4\t9\t1970-01-01T02:13:20.000000Z
                            3\t3\t1970-01-01T00:33:20.000000Z
                            3\t8\t1970-01-01T01:56:40.000000Z
                            2\t2\t1970-01-01T00:16:40.000000Z
                            2\t7\t1970-01-01T01:40:00.000000Z
                            1\t1\t1970-01-01T00:00:00.000000Z
                            1\t6\t1970-01-01T01:23:20.000000Z
                            0\t5\t1970-01-01T01:06:40.000000Z
                            0\t10\t1970-01-01T02:30:00.000000Z
                            """,
                    "x order by id1 desc, id2 asc"
            );
        });
    }

    @Test
    public void testSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,10000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by day;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:10.000000Z
                            3\t1970-01-01T00:00:20.000000Z
                            4\t1970-01-01T00:00:30.000000Z
                            5\t1970-01-01T00:00:40.000000Z
                            6\t1970-01-01T00:00:50.000000Z
                            7\t1970-01-01T00:01:00.000000Z
                            8\t1970-01-01T00:01:10.000000Z
                            9\t1970-01-01T00:01:20.000000Z
                            10\t1970-01-01T00:01:30.000000Z
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testSymbolColumnContainNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x values(null, '2024-06-12T01:00:01.000000Z');");
            execute("insert into x values(null, '2024-06-15T01:00:00.000000Z');");
            execute("insert into x values(null, '2024-06-12T01:00:02.000000Z');");

            execute("alter table x convert partition to parquet where ts >= 0");
            final String expected = """
                    id	ts
                    k1	2024-06-10T01:00:00.000000Z
                    	2024-06-12T01:00:01.000000Z
                    	2024-06-12T01:00:02.000000Z
                    	2024-06-15T01:00:00.000000Z
                    """;
            final String query = "x where id in( 'k1', null)";

            assertSql(expected, query);
        });
    }

    @Test
    public void testSymbolColumnNullFlag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x(ts) values('2024-06-10T01:00:00.000000Z');");
            execute("insert into x(id, ts) values('k1', '2024-06-11T01:00:00.000000Z');");
            execute("alter table x convert partition to parquet where ts >= 0");

            // O3
            execute("insert into x(id, ts) values('k2', '2024-06-10T02:00:00.000000Z');");

            final String expected = """
                    id	ts
                    	2024-06-10T01:00:00.000000Z
                    k2	2024-06-10T02:00:00.000000Z
                    k1	2024-06-11T01:00:00.000000Z
                    """;
            final String query = "x";

            assertSql(expected, query);
        });
    }

    @Test
    public void testSymbolColumnNullFlagOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day wal;");
            execute("insert into x(ts) values('2024-06-10T01:00:00.000000Z');");
            execute("insert into x(id, ts) values('k1', '2024-06-11T01:00:00.000000Z');");
            drainWalQueue();
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();

            // O3
            execute("insert into x(id, ts) values('k2', '2024-06-10T02:00:00.000000Z');");
            drainWalQueue();

            final String expected = """
                    id	ts
                    	2024-06-10T01:00:00.000000Z
                    k2	2024-06-10T02:00:00.000000Z
                    k1	2024-06-11T01:00:00.000000Z
                    """;
            final String query = "x";

            assertSql(expected, query);
        });
    }

    @Test
    public void testSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x (
                              id int,
                              ts timestamp,
                              name symbol
                            ) timestamp(ts) partition by day;"""
            );

            // Day 0 -- using every symbol, two nulls
            execute("insert into x values (0, 0, 'SYM_A')");
            execute("insert into x values (1, 10000000, 'SYM_A')");
            execute("insert into x values (2, 20000000, 'SYM_B_junk123')");
            execute("insert into x values (3, 30000000, 'SYM_C_junk123123123123')");
            execute("insert into x values (4, 40000000, 'SYM_D_junk12319993')");
            execute("insert into x values (5, 50000000, 'SYM_E_junk9923')");
            execute("insert into x values (6, 60000000, 'SYM_A')");
            execute("insert into x values (7, 70000000, NULL)");
            execute("insert into x values (8, 80000000, 'SYM_C_junk123123123123')");
            execute("insert into x values (9, 90000000, NULL)");

            // Day 1, NULLS, using two symbols
            execute("insert into x values (10, 86400000000, 'SYM_B_junk123')");
            execute("insert into x values (11, 86410000000, NULL)");
            execute("insert into x values (12, 86420000000, 'SYM_B_junk123')");
            execute("insert into x values (13, 86430000000, 'SYM_B_junk123')");
            execute("insert into x values (14, 86440000000, 'SYM_B_junk123')");
            execute("insert into x values (15, 86450000000, NULL)");
            execute("insert into x values (16, 86460000000, NULL)");
            execute("insert into x values (17, 86470000000, 'SYM_D_junk12319993')");
            execute("insert into x values (18, 86480000000, NULL)");
            execute("insert into x values (19, 86490000000, NULL)");

            // Day 2, no nulls, using first and last symbols only
            execute("insert into x values (20, 172800000000, 'SYM_A')");
            execute("insert into x values (21, 172810000000, 'SYM_A')");
            execute("insert into x values (22, 172820000000, 'SYM_A')");
            execute("insert into x values (23, 172830000000, 'SYM_A')");
            execute("insert into x values (24, 172840000000, 'SYM_E_junk9923')");
            execute("insert into x values (25, 172850000000, 'SYM_E_junk9923')");
            execute("insert into x values (26, 172860000000, 'SYM_E_junk9923')");

            TestUtils.LeakProneCode checkData = () -> assertQueryNoLeakCheck(
                    """
                            id\tts\tname
                            0\t1970-01-01T00:00:00.000000Z\tSYM_A
                            1\t1970-01-01T00:00:10.000000Z\tSYM_A
                            2\t1970-01-01T00:00:20.000000Z\tSYM_B_junk123
                            3\t1970-01-01T00:00:30.000000Z\tSYM_C_junk123123123123
                            4\t1970-01-01T00:00:40.000000Z\tSYM_D_junk12319993
                            5\t1970-01-01T00:00:50.000000Z\tSYM_E_junk9923
                            6\t1970-01-01T00:01:00.000000Z\tSYM_A
                            7\t1970-01-01T00:01:10.000000Z\t
                            8\t1970-01-01T00:01:20.000000Z\tSYM_C_junk123123123123
                            9\t1970-01-01T00:01:30.000000Z\t
                            10\t1970-01-02T00:00:00.000000Z\tSYM_B_junk123
                            11\t1970-01-02T00:00:10.000000Z\t
                            12\t1970-01-02T00:00:20.000000Z\tSYM_B_junk123
                            13\t1970-01-02T00:00:30.000000Z\tSYM_B_junk123
                            14\t1970-01-02T00:00:40.000000Z\tSYM_B_junk123
                            15\t1970-01-02T00:00:50.000000Z\t
                            16\t1970-01-02T00:01:00.000000Z\t
                            17\t1970-01-02T00:01:10.000000Z\tSYM_D_junk12319993
                            18\t1970-01-02T00:01:20.000000Z\t
                            19\t1970-01-02T00:01:30.000000Z\t
                            20\t1970-01-03T00:00:00.000000Z\tSYM_A
                            21\t1970-01-03T00:00:10.000000Z\tSYM_A
                            22\t1970-01-03T00:00:20.000000Z\tSYM_A
                            23\t1970-01-03T00:00:30.000000Z\tSYM_A
                            24\t1970-01-03T00:00:40.000000Z\tSYM_E_junk9923
                            25\t1970-01-03T00:00:50.000000Z\tSYM_E_junk9923
                            26\t1970-01-03T00:01:00.000000Z\tSYM_E_junk9923
                            """,
                    "x",
                    "ts",
                    true,
                    true
            );

            checkData.run();

            execute("alter table x convert partition to parquet where ts >= 0");

            checkData.run();
        });
    }

    @Test
    public void testTimeFilterMultipleRowGroupPerPartition() throws Exception {
        testTimeFilter(10);
    }

    @Test
    public void testTimeFilterSingleRowGroupPerPartition() throws Exception {
        testTimeFilter(100);
    }

    private void testArrayColTops(boolean rawArrayEncoding) throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, String.valueOf(rawArrayEncoding));

        assertMemoryLeak(() -> {
            execute("create table x (id long, ts timestamp) timestamp(ts) partition by month;");
            execute("insert into x values(1, '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values(2, '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values(3, '2024-06-12T00:00:00.000000Z');");
            execute("insert into x values(4, '2024-06-12T00:00:01.000000Z');");
            execute("insert into x values(5, '2024-06-15T00:00:00.000000Z');");
            execute("insert into x values(6, '2024-06-12T00:00:02.000000Z');");

            execute("alter table x add column a int;");
            execute("alter table x add column arr double[];");
            execute("insert into x values(7, '2024-06-10T00:00:00.000000Z', 1, array[1, 2, 3, 4, 5]);");
            execute("insert into x values(8, '2024-06-10T00:01:00.000000Z', 2, null);");
            execute("insert into x values(9, '2024-06-10T00:02:00.000000Z', 3, array[]);");
            execute("insert into x values(10, '2024-06-10T00:03:00.000000Z', 4, array[1, null, 3]);");
            execute("insert into x values(11, '2024-06-10T00:04:00.000000Z', 5, array[42]);");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(
                    """
                            id\tts\ta\tarr
                            1\t2024-06-10T00:00:00.000000Z\tnull\tnull
                            2\t2024-06-11T00:00:00.000000Z\tnull\tnull
                            3\t2024-06-12T00:00:00.000000Z\tnull\tnull
                            4\t2024-06-12T00:00:01.000000Z\tnull\tnull
                            5\t2024-06-15T00:00:00.000000Z\tnull\tnull
                            6\t2024-06-12T00:00:02.000000Z\tnull\tnull
                            7\t2024-06-10T00:00:00.000000Z\t1\t[1.0,2.0,3.0,4.0,5.0]
                            8\t2024-06-10T00:01:00.000000Z\t2\tnull
                            9\t2024-06-10T00:02:00.000000Z\t3\t[]
                            10\t2024-06-10T00:03:00.000000Z\t4\t[1.0,null,3.0]
                            11\t2024-06-10T00:04:00.000000Z\t5\t[42.0]
                            """,
                    "x order by id"
            );
        });
    }

    private void testNdArray(int dims, String arr, boolean rawArrayEncoding) throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, String.valueOf(rawArrayEncoding));

        final String columnType = "double" + "[]".repeat(dims);
        final String emptyArrayLiteral = "array[]";
        final String singleNullArrayLiteral = "array" + "[".repeat(dims) + "null" + "]".repeat(dims);
        final String singleNullArrayExpected = "[".repeat(dims) + "null" + "]".repeat(dims);

        final String arrExpr = arr
                .replaceAll(" ", "")
                .replaceAll("\n", "")
                .replace("ARRAY", "");

        assertMemoryLeak(() -> {
            execute("create table x (a1 " + columnType + ", ts timestamp) timestamp(ts) partition by month;");
            execute("insert into x values(" + arr + ", '2024-01-10T00:00:00.000000Z');");
            execute("insert into x values(" + arr + ", '2024-01-10T00:00:00.000000Z');");
            execute("insert into x values(" + arr + ", '2024-02-10T00:00:00.000000Z');");
            execute("insert into x values(" + emptyArrayLiteral + ", '2024-03-10T00:00:00.000000Z');");
            execute("insert into x values(" + singleNullArrayLiteral + ", '2024-03-10T00:00:00.000000Z');");
            execute("insert into x values(null, '2024-03-10T00:00:00.000000Z');");

            final String expected = "a1\tts\n" +
                    arrExpr + "\t2024-01-10T00:00:00.000000Z\n" +
                    arrExpr + "\t2024-01-10T00:00:00.000000Z\n" +
                    arrExpr + "\t2024-02-10T00:00:00.000000Z\n" +
                    "[]\t2024-03-10T00:00:00.000000Z\n" +
                    singleNullArrayExpected + "\t2024-03-10T00:00:00.000000Z\n" +
                    "null\t2024-03-10T00:00:00.000000Z\n";
            assertSql(expected, "x");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(expected, "x");

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, "x");
        });
    }

    private void testTimeFilter(int rowGroupSize) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(100)
                            ) timestamp(ts) partition by day;"""
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // fwd
            assertSql(
                    """
                            id\tts
                            5\t1970-01-01T01:06:40.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            8\t1970-01-01T01:56:40.000000Z
                            """,
                    "x where ts in '1970-01-01T01'"
            );
            assertSql(
                    """
                            id\tts
                            25\t1970-01-01T06:40:00.000000Z
                            26\t1970-01-01T06:56:40.000000Z
                            """,
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1d;2'"
            );
            assertSql(
                    """
                            id\tts
                            25\t1970-01-01T06:40:00.000000Z
                            26\t1970-01-01T06:56:40.000000Z
                            28\t1970-01-01T07:30:00.000000Z
                            29\t1970-01-01T07:46:40.000000Z
                            32\t1970-01-01T08:36:40.000000Z
                            33\t1970-01-01T08:53:20.000000Z
                            36\t1970-01-01T09:43:20.000000Z
                            """,
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1h;4'"
            );

            // bwd
            assertSql(
                    """
                            id\tts
                            8\t1970-01-01T01:56:40.000000Z
                            7\t1970-01-01T01:40:00.000000Z
                            6\t1970-01-01T01:23:20.000000Z
                            5\t1970-01-01T01:06:40.000000Z
                            """,
                    "x where ts in '1970-01-01T01' order by ts desc"
            );
            assertSql(
                    """
                            id\tts
                            26\t1970-01-01T06:56:40.000000Z
                            25\t1970-01-01T06:40:00.000000Z
                            """,
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1d;2' order by ts desc"
            );
            assertSql(
                    """
                            id\tts
                            36\t1970-01-01T09:43:20.000000Z
                            33\t1970-01-01T08:53:20.000000Z
                            32\t1970-01-01T08:36:40.000000Z
                            29\t1970-01-01T07:46:40.000000Z
                            28\t1970-01-01T07:30:00.000000Z
                            26\t1970-01-01T06:56:40.000000Z
                            25\t1970-01-01T06:40:00.000000Z
                            """,
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1h;4' order by ts desc"
            );
        });
    }
}
