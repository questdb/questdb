/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Cast-to-string / cast-to-varchar coverage for the NOT NULL FuncNotNull paths
 * of CastIPv4ToStrFunctionFactory, CastUuidToStrFunctionFactory,
 * CastLong256ToStrFunctionFactory, CastCharToStrFunctionFactory,
 * CastDecimalToStrFunctionFactory (DECIMAL 64 / 128 / 256 variants),
 * CastDateToStrFunctionFactory, CastFloatToStrFunctionFactory,
 * CastIntervalToStrFunctionFactory, and their varchar siblings.
 * <p>
 * NotNullColumnTest already asserts the INT / LONG / DOUBLE / TIMESTAMP shape
 * (testCastToStringOnNotNullColumn). The types covered here go through
 * dedicated cast factories that pick FuncNotNull when the source column is
 * NOT NULL — this test exercises both that picked path with the type's null
 * sentinel as the input and the nullable Func counterpart (where the factory
 * still falls through to a NULL result for a NULL row).
 */
public class NotNullCastExtendedTypesTest extends AbstractCairoTest {

    @Test
    public void testCastCharNotNullSentinelToStringRendersEmpty() throws Exception {
        // CHAR null sentinel is char(0). FuncNotNull.getStrA writes the char to a
        // StringSink unconditionally — char(0) is the NUL character with no
        // visible representation, so the cell renders as empty (but the row
        // still appears, distinguishing it from a real NULL which would render
        // identically but mean something different upstream).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c CHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        ('A', '2024-01-02')
                    """);

            assertSql(
                    """
                            c_str
                            \0
                            A
                            """,
                    "SELECT c::string c_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            c_v
                            \0
                            A
                            """,
                    "SELECT c::varchar c_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastIPv4NotNullSentinelToStringRendersZeroQuad() throws Exception {
        // IPv4_NULL = 0. Numbers.intToIPv4Sink emits "0.0.0.0" for the sentinel
        // when called unconditionally (FuncNotNull skips the != IPv4_NULL guard).
        // The Func (nullable) variant returns null instead, which would render
        // as empty. This test pins the NOT NULL behaviour to "0.0.0.0".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ip IPv4 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        ('192.168.1.1', '2024-01-02')
                    """);

            assertSql(
                    """
                            ip_str
                            0.0.0.0
                            192.168.1.1
                            """,
                    "SELECT ip::string ip_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            ip_v
                            0.0.0.0
                            192.168.1.1
                            """,
                    "SELECT ip::varchar ip_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastLong256NotNullSentinelToStringRendersRawHex() throws Exception {
        // LONG256 null sentinel is (LONG_NULL, LONG_NULL, LONG_NULL, LONG_NULL)
        // i.e. four copies of 0x8000000000000000. The FuncNotNull cast paths
        // for both string and varchar pass checkNull=false to
        // Numbers.appendLong256, matching CursorPrinter's NOT NULL branch:
        // the sentinel is printed as its raw hex bit pattern, not stripped.
        // SELECT col and SELECT col::string render the same value for a
        // sentinel row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l256 LONG256 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        ('0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20', '2024-01-02')
                    """);

            assertSql(
                    """
                            l_str
                            0x8000000000000000800000000000000080000000000000008000000000000000
                            0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20
                            """,
                    "SELECT l256::string l_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            l_v
                            0x8000000000000000800000000000000080000000000000008000000000000000
                            0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20
                            """,
                    "SELECT l256::varchar l_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastUuidNotNullSentinelToStringRendersZeroUuid() throws Exception {
        // UUID null sentinel is (LONG_NULL, LONG_NULL) — i.e. (0x80000000_00000000, 0x80000000_00000000).
        // FuncNotNull.getStrA calls Numbers.appendUuid(lo, hi) directly, which
        // formats the 128-bit pattern as a hyphenated hex string regardless of
        // its value. So the sentinel renders as the canonical
        // "80000000-0000-0000-8000-000000000000" string (the same value the
        // CursorPrinter prints in NotNullColumnTest#testNotNullUuidSentinelPrintsValue).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (uu UUID NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-02')
                    """);

            assertSql(
                    """
                            u_str
                            80000000-0000-0000-8000-000000000000
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """,
                    "SELECT uu::string u_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            u_v
                            80000000-0000-0000-8000-000000000000
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """,
                    "SELECT uu::varchar u_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastDecimalNotNullColumnToStringAndVarchar() throws Exception {
        // Each DECIMAL precision tier routes the cast factory to a different
        // NotNull class:
        //   DECIMAL(<=18)   -> Func64NotNull  (via Decimal64LoaderFunctionFactory)
        //   DECIMAL(19-38)  -> Func128NotNull
        //   DECIMAL(39-76)  -> FuncNotNull    (256-bit)
        // FuncNotNull variants skip the decimal.isNull() guard and always call
        // Decimal{64,128,256}.toSink. For a row produced by INSERT NULL into a
        // NOT NULL column, the stored bit pattern is the type's null sentinel,
        // and toSink renders an empty cell (QuestDB prints the decimal NULL
        // sentinel as an empty value, same as CursorPrinter's "dec*\tNULL"
        // convention seen in NotNullColumnTest#testCastToStringOnNotNullColumn).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        d64 DECIMAL(9, 2) NOT NULL,
                        d128 DECIMAL(30, 2) NOT NULL,
                        d256 DECIMAL(60, 2) NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (NULL, NULL, NULL, '2024-01-01'),
                        (123.45::DECIMAL(9, 2), 99999.99::DECIMAL(30, 2), 12345.67::DECIMAL(60, 2), '2024-01-02')
                    """);

            // Func*NotNull.getStrA (DECIMAL64 / DECIMAL128 / DECIMAL256).
            assertSql(
                    """
                            d64_s\td128_s\td256_s
                            \t\t
                            123.45\t99999.99\t12345.67
                            """,
                    "SELECT d64::string d64_s, d128::string d128_s, d256::string d256_s FROM t ORDER BY ts"
            );

            // Func*NotNull.getVarcharA on the same paths.
            assertSql(
                    """
                            d64_v\td128_v\td256_v
                            \t\t
                            123.45\t99999.99\t12345.67
                            """,
                    "SELECT d64::varchar d64_v, d128::varchar d128_v, d256::varchar d256_v FROM t ORDER BY ts"
            );

            // DISTINCT drives both getStrA and getStrB (record-staging path).
            assertSql(
                    """
                            d64_s
                            
                            123.45
                            """,
                    "SELECT DISTINCT d64::string d64_s FROM t ORDER BY d64_s"
            );
        });
    }

    @Test
    public void testCastDecimalNullableColumnToStringAndVarchar() throws Exception {
        // Nullable sibling of the NOT NULL test. Each DECIMAL tier picks the
        // Func / Func128 / Func64 class, which check decimal.isNull() and
        // return null for the sentinel — this covers the null-branch of
        // getStrA / getVarcharA.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        d64 DECIMAL(9, 2),
                        d128 DECIMAL(30, 2),
                        d256 DECIMAL(60, 2),
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (NULL, NULL, NULL, '2024-01-01'),
                        (-0.25::DECIMAL(9, 2), -9999.99::DECIMAL(30, 2), 42.00::DECIMAL(60, 2), '2024-01-02')
                    """);

            assertSql(
                    """
                            d64_s\td128_s\td256_s
                            \t\t
                            -0.25\t-9999.99\t42.00
                            """,
                    "SELECT d64::string d64_s, d128::string d128_s, d256::string d256_s FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            d64_v\td128_v\td256_v
                            \t\t
                            -0.25\t-9999.99\t42.00
                            """,
                    "SELECT d64::varchar d64_v, d128::varchar d128_v, d256::varchar d256_v FROM t ORDER BY ts"
            );

            // DISTINCT drives getStrB on the nullable path too.
            assertSql(
                    """
                            d256_s
                            
                            42.00
                            """,
                    "SELECT DISTINCT d256::string d256_s FROM t ORDER BY d256_s"
            );
        });
    }

    @Test
    public void testCastDateNotNullSentinelToStringRendersRawLong() throws Exception {
        // DATE null sentinel is Long.MIN_VALUE. FuncNotNull.format special-cases
        // MIN_VALUE to fall back to Numbers.append (decimal integer) rather than
        // StringSink.putISODateMillis, which would short-circuit on MIN_VALUE
        // and silently emit "null". This mirrors CursorPrinter's NOT NULL
        // branch for DATE and is the behavioural contract for a NOT NULL DATE
        // column that somehow stored the sentinel (e.g., via INSERT NULL).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DATE NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        ('2024-06-15T12:00:00.000Z', '2024-01-02')
                    """);

            assertSql(
                    """
                            d_str
                            -9223372036854775808
                            2024-06-15T12:00:00.000Z
                            """,
                    "SELECT d::string d_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            d_v
                            -9223372036854775808
                            2024-06-15T12:00:00.000Z
                            """,
                    "SELECT d::varchar d_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastFloatNotNullSentinelToStringRendersNaN() throws Exception {
        // FLOAT null sentinel is NaN. FuncNotNull.getStrA calls sinkA.put(float)
        // unconditionally, which emits "NaN" for NaN inputs — matching the
        // SELECT * output pinned in NotNullColumnTest#testNotNullFloatSpecialValues.
        // The nullable Func variant checks Numbers.isNull and returns null
        // instead; that divergence is what FuncNotNull pins.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (f FLOAT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        (1.5::FLOAT, '2024-01-02'),
                        (CAST('Infinity' AS FLOAT), '2024-01-03')
                    """);

            assertSql(
                    """
                            f_str
                            NaN
                            1.5
                            Infinity
                            """,
                    "SELECT f::string f_str FROM t ORDER BY ts"
            );

            assertSql(
                    """
                            f_v
                            NaN
                            1.5
                            Infinity
                            """,
                    "SELECT f::varchar f_v FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testCastIntervalExpressionToStringAndVarchar() throws Exception {
        // INTERVAL is not a persisted column type, so NotNullCastExtendedTypes
        // exercises CastIntervalToStr/VarcharFunctionFactory via an interval
        // expression whose arguments are columns. The resulting Function is
        // non-constant and defaults to isNotNull() == false, which routes the
        // cast factory through its Func (nullable) class. This covers the
        // main getStrA / getVarcharA path plus the Interval.NULL.equals guard
        // for a row where the interval expression yielded NULL (produced by
        // either timestamp argument being NULL sentinel).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts1 TIMESTAMP, ts2 TIMESTAMP, tsOrder TIMESTAMP NOT NULL) TIMESTAMP(tsOrder) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-10T00:00:00.000Z', '2024-06-11T00:00:00.000Z', '2024-01-01'),
                        (NULL, '2024-06-12T00:00:00.000Z', '2024-01-02')
                    """);

            assertSql(
                    """
                            i_str
                            ('2024-06-10T00:00:00.000Z', '2024-06-11T00:00:00.000Z')
                            
                            """,
                    "SELECT interval(ts1, ts2)::string i_str FROM t ORDER BY tsOrder"
            );

            assertSql(
                    """
                            i_v
                            ('2024-06-10T00:00:00.000Z', '2024-06-11T00:00:00.000Z')
                            
                            """,
                    "SELECT interval(ts1, ts2)::varchar i_v FROM t ORDER BY tsOrder"
            );
        });
    }
}
