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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests that aggregate functions (SUM, AVG, MIN, MAX, COUNT) produce correct
 * results for UINT16, UINT32, and UINT64 columns.
 * <p>
 * These types use bitmap-based null support (not sentinel values), and their
 * unsigned value ranges include bit patterns that coincide with the signed
 * null sentinels (INT_NULL = 0x80000000, LONG_NULL = 0x8000000000000000).
 * <p>
 * Known issues these tests expose:
 * 1. SUM/AVG use signed getShort()/getInt()/getLong() without unsigned promotion
 * 2. SumIntGroupByFunction checks value != INT_NULL (sentinel), which misidentifies
 *    the valid UINT32 value 2_147_483_648 as null
 * 3. SumLongGroupByFunction checks value != LONG_NULL (sentinel), which misidentifies
 *    the valid UINT64 value 9_223_372_036_854_775_808 as null
 * 4. MinIntGroupByFunction/MaxIntGroupByFunction use signed comparison, giving
 *    wrong results for UINT32 values above 2^31-1
 * 5. SumShortGroupByFunction calls getShort() which sign-extends UINT16 values
 *    above 32_767, corrupting the accumulator
 */
public class UnsignedAggregateTest extends AbstractCairoTest {

    // ================================================================
    // UINT16 — getShort() sign-extends values > 32_767
    // SumShortGroupByFunction stores: mapValue.putLong(idx, arg.getShort(rec))
    // For UINT16 value 65_535, getShort() returns -1, widened to -1L
    // ================================================================

    @Test
    public void testSumUInt16HighValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT16)");
            execute("INSERT INTO t VALUES (1::UINT16), (32768::UINT16), (65535::UINT16)");
            // Correct unsigned: 1 + 32_768 + 65_535 = 98_304
            // Bug: getShort() returns 1, -32768, -1 → sum = -32768
            assertSql("s\n98304\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testAvgUInt16HighValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT16)");
            execute("INSERT INTO t VALUES (65535::UINT16), (65535::UINT16)");
            // Correct unsigned: avg(65_535, 65_535) = 65535.0
            // Bug: getShort() returns -1 for both → avg(-1, -1) = -1.0
            assertSql("a\n65535.0\n", "SELECT AVG(val) AS a FROM t");
        });
    }

    @Test
    public void testSumUInt16SkipsActualNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT16)");
            execute("INSERT INTO t VALUES (10::UINT16), (NULL), (20::UINT16)");
            // Bitmap nulls should be skipped; non-null values summed correctly
            assertSql("s\n30\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testCountUInt16WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT16)");
            execute("INSERT INTO t VALUES (1::UINT16), (32768::UINT16), (NULL)");
            // 32_768 is a valid UINT16 value (was previously SHORT_NULL sentinel)
            assertSql("c\n2\n", "SELECT COUNT(val) AS c FROM t");
        });
    }

    // ================================================================
    // UINT32 — Sentinel collision: INT_NULL = 0x80000000 = 2_147_483_648 unsigned
    // SumIntGroupByFunction checks: if (value != Numbers.INT_NULL)
    // This falsely treats the valid UINT32 value 2_147_483_648 as null.
    // Also: getInt() returns signed int, and sum promotion is signed.
    // ================================================================

    @Test
    public void testSumUInt32SentinelCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            // 2_147_483_648 = 0x80000000 = Integer.MIN_VALUE = Numbers.INT_NULL as signed
            execute("INSERT INTO t VALUES (1), (2147483648::UINT32), (100)");
            // Correct: 1 + 2_147_483_648 + 100 = 2_147_483_749
            // Bug: SumIntGroupByFunction treats 2_147_483_648 as null → SUM(1, 100) = 101
            assertSql("s\n2147483749\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testSumUInt32SignedPromotion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (1), (4294967295::UINT32)");
            // Correct: 1 + 4_294_967_295 = 4_294_967_296
            // Bug: getInt() returns -1, sign-extended to -1L → sum = 1 + (-1) = 0
            assertSql("s\n4294967296\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testAvgUInt32Correctness() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (0), (4294967294::UINT32)");
            // AVG for UINT32 actually works correctly because the INT avg factory
            // promotes to DOUBLE via getDouble(), which returns unsigned values.
            // 2.147483647E9 = 2_147_483_647.0 (correct unsigned avg)
            assertSql("a\n2.147483647E9\n", "SELECT AVG(val) AS a FROM t");
        });
    }

    @Test
    public void testMinUInt32UnsignedSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (10), (3000000000::UINT32)");
            // Correct unsigned MIN: 10
            // Bug: signed comparison → min(10, -1_294_967_296) = -1_294_967_296
            //      displayed as INT, so shows as "-1294967296"
            assertSql("m\n10\n", "SELECT MIN(val) AS m FROM t");
        });
    }

    @Test
    public void testMaxUInt32UnsignedSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (10), (3000000000::UINT32)");
            // Correct unsigned MAX: 3_000_000_000
            // Bug: signed comparison → max(10, -1_294_967_296) = 10
            assertSql("m\n3000000000\n", "SELECT MAX(val) AS m FROM t");
        });
    }

    @Test
    public void testCountUInt32SentinelNotTreatedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            // 2_147_483_648 is a valid UINT32 value, NOT null
            execute("INSERT INTO t VALUES (1), (2147483648::UINT32), (NULL)");
            assertSql("c\n2\n", "SELECT COUNT(val) AS c FROM t");
        });
    }

    @Test
    public void testSumUInt32SkipsActualNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (10), (NULL), (20)");
            assertSql("s\n30\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testSumUInt32AllNullReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (NULL), (NULL)");
            assertSql("s\nnull\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testMinUInt32WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (NULL), (100), (NULL), (50)");
            // Nulls should be skipped; MIN of non-null values = 50
            // Bug: MinIntGroupByFunction.computeFirst() doesn't check isNull(),
            //      so NULL rows contribute 0 (raw data value) → min becomes 0
            assertSql("m\n50\n", "SELECT MIN(val) AS m FROM t");
        });
    }

    @Test
    public void testMaxUInt32WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("INSERT INTO t VALUES (NULL), (100), (NULL), (50)");
            // Nulls should be skipped; MAX of non-null values = 100
            assertSql("m\n100\n", "SELECT MAX(val) AS m FROM t");
        });
    }

    // ================================================================
    // UINT64 — Sentinel collision: LONG_NULL = 0x8000000000000000
    //          = 9_223_372_036_854_775_808 unsigned
    // SumLongGroupByFunction checks: if (value != Numbers.LONG_NULL)
    // Also: signed long arithmetic gives wrong results for values > 2^63-1
    // ================================================================

    @Test
    public void testSumUInt64SentinelCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            // 9_223_372_036_854_775_808 = 0x8000000000000000 = Long.MIN_VALUE = LONG_NULL
            execute("""
                    INSERT INTO t VALUES
                    ((-9223372036854775807 - 1)::UINT64),
                    (1::UINT64)
                    """);
            // If sentinel collision: SUM = 1 (9_223_372_036_854_775_808 treated as null)
            // Correct: SUM = 9_223_372_036_854_775_808 + 1 = 9_223_372_036_854_775_809
            //          (as signed long = -9_223_372_036_854_775_807)
            // The exact representation depends on whether SUM returns signed long.
            // Key assertion: result must NOT be 1 (proving no sentinel collision).
            assertSql("s\n-9223372036854775807\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testSumUInt64SmallValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (100::UINT64), (200::UINT64), (300::UINT64)");
            // Small values that fit in signed long — should work correctly
            assertSql("s\n600\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testMinUInt64UnsignedSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (10::UINT64), ((-9223372036854775807 - 1)::UINT64)");
            // Correct unsigned MIN: 10
            // Bug: signed comparison → min(10, -9_223_372_036_854_775_808) picks the negative
            assertSql("m\n10\n", "SELECT MIN(val) AS m FROM t");
        });
    }

    @Test
    public void testMaxUInt64UnsignedSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (10::UINT64), ((-1)::UINT64)");
            // Correct unsigned MAX: 18_446_744_073_709_551_615 (= -1 as signed = max UINT64)
            // Bug: signed comparison → max(10, -1) = 10
            assertSql("m\n18446744073709551615\n", "SELECT MAX(val) AS m FROM t");
        });
    }

    @Test
    public void testCountUInt64SentinelNotTreatedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (1::UINT64), ((-9223372036854775807 - 1)::UINT64), (NULL)");
            // 9_223_372_036_854_775_808 is a valid UINT64 value, NOT null
            assertSql("c\n2\n", "SELECT COUNT(val) AS c FROM t");
        });
    }

    @Test
    public void testSumUInt64SkipsActualNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (10::UINT64), (NULL), (20::UINT64)");
            assertSql("s\n30\n", "SELECT SUM(val) AS s FROM t");
        });
    }

    @Test
    public void testAvgUInt64SmallValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("INSERT INTO t VALUES (100::UINT64), (200::UINT64)");
            assertSql("a\n150.0\n", "SELECT AVG(val) AS a FROM t");
        });
    }

    // ================================================================
    // GROUP BY — aggregate functions with grouping key
    // Tests that aggregates produce correct results per-group
    // ================================================================

    @Test
    public void testSumUInt32GroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (grp INT, val UINT32)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 100),
                    (1, 4294967295::UINT32),
                    (2, 2147483648::UINT32),
                    (2, 1)
                    """);
            // Group 1: 100 + 4_294_967_295 = 4_294_967_395
            // Group 2: 2_147_483_648 + 1 = 2_147_483_649
            assertSql(
                    """
                            grp\ts
                            1\t4294967395
                            2\t2147483649
                            """,
                    "SELECT grp, SUM(val) AS s FROM t ORDER BY grp"
            );
        });
    }

    @Test
    public void testMinMaxUInt32GroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (grp INT, val UINT32)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 10),
                    (1, 3000000000::UINT32),
                    (2, 4294967295::UINT32),
                    (2, 100)
                    """);
            // Group 1 unsigned: MIN=10, MAX=3_000_000_000
            // Group 2 unsigned: MIN=100, MAX=4_294_967_295
            assertSql(
                    """
                            grp\tmi\tma
                            1\t10\t3000000000
                            2\t100\t4294967295
                            """,
                    "SELECT grp, MIN(val) AS mi, MAX(val) AS ma FROM t ORDER BY grp"
            );
        });
    }

    @Test
    public void testSumUInt16GroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (grp INT, val UINT16)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 1::UINT16),
                    (1, 65535::UINT16),
                    (2, 32768::UINT16),
                    (2, 32768::UINT16)
                    """);
            // Group 1: 1 + 65_535 = 65_536
            // Group 2: 32_768 + 32_768 = 65_536
            assertSql(
                    """
                            grp\ts
                            1\t65536
                            2\t65536
                            """,
                    "SELECT grp, SUM(val) AS s FROM t ORDER BY grp"
            );
        });
    }

    // ================================================================
    // Mixed unsigned and null — comprehensive scenario
    // ================================================================

    @Test
    public void testUInt32AggregatesComprehensive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT32)");
            execute("""
                    INSERT INTO t VALUES
                    (0),
                    (1),
                    (2147483647::UINT32),
                    (2147483648::UINT32),
                    (4294967295::UINT32),
                    (NULL)
                    """);
            // SUM: 0 + 1 + 2_147_483_647 + 2_147_483_648 + 4_294_967_295 = 8_589_934_591
            assertSql("s\n8589934591\n", "SELECT SUM(val) AS s FROM t");
            // COUNT: 5 non-null values
            assertSql("c\n5\n", "SELECT COUNT(val) AS c FROM t");
            // MIN: 0
            assertSql("m\n0\n", "SELECT MIN(val) AS m FROM t");
            // MAX: 4_294_967_295
            assertSql("m\n4294967295\n", "SELECT MAX(val) AS m FROM t");
            // AVG: 8_589_934_591 / 5 = 1_717_986_918.2
            assertSql("a\n1.7179869182E9\n", "SELECT AVG(val) AS a FROM t");
        });
    }

    @Test
    public void testUInt64AggregatesComprehensive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT64)");
            execute("""
                    INSERT INTO t VALUES
                    (0::UINT64),
                    (1::UINT64),
                    (100::UINT64),
                    (NULL)
                    """);
            // Small values that fit in signed long — no overflow ambiguity
            assertSql("s\n101\n", "SELECT SUM(val) AS s FROM t");
            assertSql("c\n3\n", "SELECT COUNT(val) AS c FROM t");
            assertSql("m\n0\n", "SELECT MIN(val) AS m FROM t");
            assertSql("m\n100\n", "SELECT MAX(val) AS m FROM t");
        });
    }

    @Test
    public void testUInt16AggregatesComprehensive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val UINT16)");
            execute("""
                    INSERT INTO t VALUES
                    (0::UINT16),
                    (1::UINT16),
                    (32767::UINT16),
                    (32768::UINT16),
                    (65535::UINT16),
                    (NULL)
                    """);
            // SUM: 0 + 1 + 32_767 + 32_768 + 65_535 = 131_071
            assertSql("s\n131071\n", "SELECT SUM(val) AS s FROM t");
            // COUNT: 5 non-null values
            assertSql("c\n5\n", "SELECT COUNT(val) AS c FROM t");
            // MIN: 0
            assertSql("m\n0\n", "SELECT MIN(val) AS m FROM t");
            // MAX: 65_535
            assertSql("m\n65535\n", "SELECT MAX(val) AS m FROM t");
            // AVG: 131_071 / 5 = 26_214.2
            assertSql("a\n26214.2\n", "SELECT AVG(val) AS a FROM t");
        });
    }
}
