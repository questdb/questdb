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
 * CastLong256ToStrFunctionFactory, and CastCharToStrFunctionFactory (and the
 * varchar siblings).
 * <p>
 * NotNullColumnTest already asserts the INT / LONG / DOUBLE / TIMESTAMP shape
 * (testCastToStringOnNotNullColumn). The four types covered here go through
 * dedicated cast factories that pick FuncNotNull when the source column is
 * NOT NULL — this test exercises that picked path with the type's null
 * sentinel as the input.
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
}
