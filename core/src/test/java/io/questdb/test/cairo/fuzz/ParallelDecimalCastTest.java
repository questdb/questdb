/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ParallelDecimalCastTest extends AbstractCairoTest {
    public ParallelDecimalCastTest() {
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testDecimalCastDoubleRoundTrip() throws Exception {
        assertDecimalCasting("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            rnd_symbol(5, 4, 4, 2) AS sym,
                            cast(((x % 200000) - 100000) * 0.001 as DECIMAL(18,6)) AS dec64,
                            cast(((x % 200000000) - 100000000) * 0.00001 as DECIMAL(30,10)) AS dec128,
                            cast(((x % 200000000) - 100000000) * 0.0000001 as DECIMAL(50,18)) AS dec256
                          FROM long_sequence(2000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym,
                               sum(cast(cast(dec64 as double) as DECIMAL(18,6))) AS dec64_roundtrip,
                               sum(cast(cast(dec128 as double) as DECIMAL(30,10))) AS dec128_roundtrip,
                               sum(cast(cast(dec256 as double) as DECIMAL(38,12))) AS dec256_roundtrip
                        FROM x
                        GROUP BY sym
                        ORDER BY sym""", """
                        sym\tdec64_roundtrip\tdec128_roundtrip\tdec256_roundtrip
                        \t-65037.536000\t-656993.3753600000\t-6569.933753599838
                        CPSW\t-27131.350000\t-273997.3135000000\t-2739.973134999927
                        HYRX\t-25045.585000\t-252997.4558500000\t-2529.974558499936
                        PEHN\t-23942.021000\t-241997.4202100000\t-2419.974202099944
                        RXGZ\t-29298.103000\t-295996.9810300000\t-2959.969810299919
                        VTJW\t-27544.405000\t-277997.4440500000\t-2779.974440499926
                        """
        );
    }

    @Test
    public void testDecimalCastFromIntegers() throws Exception {
        assertDecimalCasting(
                """
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            rnd_symbol(5, 4, 4, 2) AS sym,
                            ((x % 100000) - 50000)::int AS int_col,
                            ((x % 2000) - 1000)::short AS short_col,
                            ((x % 200) - 100)::byte AS byte_col
                          FROM long_sequence(2000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                """
                        SELECT sym,
                               sum(cast(int_col as DECIMAL(18,4))) AS int_decimal,
                               sum(cast(short_col as DECIMAL(10,2))) AS short_decimal,
                               sum(cast(byte_col as DECIMAL(8,2))) AS byte_decimal
                        FROM x
                        GROUP BY sym
                        ORDER BY sym
                        """,
                """
                        sym\tint_decimal\tshort_decimal\tbyte_decimal
                        \t-32187536.0000\t5464.00\t1364.00
                        CPSW\t-13431350.0000\t-5350.00\t-950.00
                        HYRX\t-12395585.0000\t-585.00\t-1285.00
                        PEHN\t-11842021.0000\t15979.00\t-421.00
                        RXGZ\t-14498103.0000\t5897.00\t297.00
                        VTJW\t-13644405.0000\t-22405.00\t-5.00
                        """
        );
    }

    @Test
    public void testDecimalCastToByte() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 2) - 1) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 20) - 10) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 200) - 100) as DECIMAL(50,18)) AS dec256,
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as byte)) AS dec64_byte,
                       sum(cast(dec128 as byte)) AS dec128_byte,
                       sum(cast(dec256 as byte)) AS dec256_byte
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_byte\tdec128_byte\tdec256_byte
                \t-329\t-466\t1364
                CPSW\t-138\t-10\t-950
                HYRX\t-136\t-135\t-1285
                PEHN\t-115\t-141\t-421
                RXGZ\t-141\t-83\t297
                VTJW\t-141\t-165\t-5
                """);
    }

    @Test
    public void testDecimalCastToDouble() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 200000) - 100000) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 200000000) - 100000000) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 200000000) - 100000000) as DECIMAL(50,18)) AS dec256
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as double)) AS dec64_double,
                       sum(cast(dec128 as double)) AS dec128_double,
                       sum(cast(dec256 as double)) AS dec256_double
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_double\tdec128_double\tdec256_double
                \t-6.5037536E7\t-6.5699337536E10\t-6.5699337536E10
                CPSW\t-2.713135E7\t-2.739973135E10\t-2.739973135E10
                HYRX\t-2.5045585E7\t-2.5299745585E10\t-2.5299745585E10
                PEHN\t-2.3942021E7\t-2.4199742021E10\t-2.4199742021E10
                RXGZ\t-2.9298103E7\t-2.9599698103E10\t-2.9599698103E10
                VTJW\t-2.7544405E7\t-2.7799744405E10\t-2.7799744405E10
                """);

    }

    @Test
    public void testDecimalCastToFloat() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 200) - 100) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 2000) - 1000) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 20000) - 10000) as DECIMAL(50,18)) AS dec256
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as float)) AS dec64_float,
                       sum(cast(dec128 as float)) AS dec128_float,
                       sum(cast(dec256 as float)) AS dec256_float
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_float\tdec128_float\tdec256_float
                \t1364.0\t5464.0\t-5907536.0
                CPSW\t-950.0\t-5350.0\t-2471350.0
                HYRX\t-1285.0\t-585.0\t-2275585.0
                PEHN\t-421.0\t15979.0\t-2162021.0
                RXGZ\t297.0\t5897.0\t-2658103.0
                VTJW\t-5.0\t-22405.0\t-2524405.0
                """);

    }

    @Test
    public void testDecimalCastToInt() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 200000) - 100000) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 2000000) - 1000000) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 20000000) - 10000000) as DECIMAL(50,18)) AS dec256,
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as int)) AS dec64_int,
                       sum(cast(dec128 as int)) AS dec128_int,
                       sum(cast(dec256 as int)) AS dec256_int
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_int\tdec128_int\tdec256_int
                \t-65037536\t-656337536\t-6569337536
                CPSW\t-27131350\t-273731350\t-2739731350
                HYRX\t-25045585\t-252745585\t-2529745585
                PEHN\t-23942021\t-241742021\t-2419742021
                RXGZ\t-29298103\t-295698103\t-2959698103
                VTJW\t-27544405\t-277744405\t-2779744405
                """);
    }

    @Test
    public void testDecimalCastToLong() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 200000) - 100000) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 200000000) - 100000000) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 200000000) - 100000000) as DECIMAL(50,18)) AS dec256,
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as long)) AS dec64_long,
                       sum(cast(dec128 as long)) AS dec128_long,
                       sum(cast(dec256 as long)) AS dec256_long
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_long\tdec128_long\tdec256_long
                \t-65037536\t-65699337536\t-65699337536
                CPSW\t-27131350\t-27399731350\t-27399731350
                HYRX\t-25045585\t-25299745585\t-25299745585
                PEHN\t-23942021\t-24199742021\t-24199742021
                RXGZ\t-29298103\t-29599698103\t-29599698103
                VTJW\t-27544405\t-27799744405\t-27799744405
                """);
    }

    @Test
    public void testDecimalCastToShort() throws Exception {
        assertDecimalCasting("""
                CREATE TABLE x AS (
                  SELECT
                    timestamp_sequence(0, 1000000) AS ts,
                    rnd_symbol(5, 4, 4, 2) AS sym,
                    cast(((x % 200) - 100) as DECIMAL(18,6)) AS dec64,
                    cast(((x % 2000) - 1000) as DECIMAL(30,10)) AS dec128,
                    cast(((x % 20000) - 10000) as DECIMAL(50,18)) AS dec256,
                  FROM long_sequence(2000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", """
                SELECT sym,
                       sum(cast(dec64 as short)) AS dec64_short,
                       sum(cast(dec128 as short)) AS dec128_short,
                       sum(cast(dec256 as short)) AS dec256_short
                FROM x
                GROUP BY sym
                ORDER BY sym""", """
                sym\tdec64_short\tdec128_short\tdec256_short
                \t1364\t5464\t-5907536
                CPSW\t-950\t-5350\t-2471350
                HYRX\t-1285\t-585\t-2275585
                PEHN\t-421\t15979\t-2162021
                RXGZ\t297\t5897\t-2658103
                VTJW\t-5\t-22405\t-2524405
                """);
    }

    private void assertDecimalCasting(String ddl, String sql, String expected) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);

            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
                                compiler,
                                ddl,
                                sqlExecutionContext
                        );
                        TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, expected);
                    },
                    configuration,
                    LOG
            );
        });
    }
}
