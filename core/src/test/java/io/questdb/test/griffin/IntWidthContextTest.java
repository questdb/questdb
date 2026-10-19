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

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Verifies that wider consumers observe the single wrapped value of an INT expression.
 */
public class IntWidthContextTest extends AbstractCairoTest {

    // 1_720_468_802 * 1_000_000 needs 51 bits, so it wraps to this.
    private static final String SECS_WRAPPED = "-607497088";
    // 2_000_000_000 + 2_000_000_000 needs 32 bits, so it wraps to this.
    private static final String SUM_WRAPPED = "-294967296";

    @Test
    public void testComparisonAgainstFunctionValuedPeerReadsTheWrappedValue() throws Exception {
        // A comparison takes the width of its OPERATION from the peer's declared type, but the
        // operand it compares has already wrapped. When the peer is a bare column that type is
        // available syntactically; when it is a function - abs(l), now(), dateadd(...) - it is not.
        // Every spelling has to agree, because they are the same predicate over the same value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cmp (i INT, j INT, l LONG)");
            execute("INSERT INTO cmp VALUES (2_000_000_000, 2_000_000_000, 0)");

            // bare LONG column peer: the wrapped sum is negative, so it fails the predicate
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > l")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");

            // function-valued LONG peer: the same predicate, the same answer
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > abs(l)")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > l + 0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");

            // and the INT-peer spelling agrees with all of them, which is the whole point: the
            // width of the bound no longer decides the width of the arithmetic
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > 0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");

            // widening an operand moves the arithmetic itself to 64 bits, and every peer follows
            assertQuery("SELECT count() AS c FROM cmp WHERE i::long + j > l")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() AS c FROM cmp WHERE i::long + j > abs(l)")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
        });
    }

    @Test
    public void testConditionalWithWideArmReadsTheWrappedValue() throws Exception {
        // CaseCommon escalates (INT, LONG) to LONG and there is no (INT, LONG) cast factory, so
        // getCastFunction hands the INT arm back unchanged and the LONG conditional reads it through
        // getLong(). The 64-bit context comes from the SIBLING arm, with no cast anywhere in the
        // expression - and it changes the conditional's result type without changing the arm's value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cnd (i INT, j INT, l LONG)");
            execute("INSERT INTO cnd VALUES (2_000_000_000, 2_000_000_000, 0)");

            // COALESCE: the LONG sibling escalates the result TYPE; the INT arm's value is the wrap
            assertQuery("SELECT coalesce(i + j, l) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");

            // CASE: same escalation through the ELSE arm
            assertQuery("SELECT (CASE WHEN true THEN i + j ELSE l END) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");

            // NULLIF resolves nullif(LL) and both compares and returns the sign-extended wrap
            assertQuery("SELECT nullif(i + j, l) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");

            // the INT-sibling spelling now agrees on the value and differs only in declared type
            assertQuery("SELECT coalesce(i + j, 0) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n" + SUM_WRAPPED + "\n");
            assertQuery("SELECT (CASE WHEN true THEN i + j ELSE 0 END) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n" + SUM_WRAPPED + "\n");
        });
    }

    @Test
    public void testMixedWidthArithmeticReadsTheWrappedValue() throws Exception {
        // INT arithmetic next to a 64-bit operand. The LONG row of ColumnType.OVERLOAD_PRIORITY
        // does not contain INT, so +(II) cannot match a (INT, LONG) pair and +(LL) is selected;
        // AddLongFunctionFactory then reads the INT operand through getLong(), which sign-extends
        // the value the inner INT arithmetic already wrapped to. The OUTER operation runs at 64
        // bits; the inner one does not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mix (secs INT, i INT, j INT, l LONG, one LONG)");
            execute("INSERT INTO mix VALUES (1_720_468_802, 2_000_000_000, 2_000_000_000, 0, 1)");

            // LONG column peer, on both sides of the operator
            assertQuery("SELECT (i + j) + l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");
            assertQuery("SELECT l + (i + j) AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");
            assertQuery("SELECT (i + j) * one AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");
            assertQuery("SELECT (i + j) - l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");

            // the issue #4752 expression next to a LONG offset: the peer does not rescue it
            assertQuery("SELECT (secs * 1_000_000) + l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SECS_WRAPPED + "\n");

            // a bitwise operator promotes the same way
            assertQuery("SELECT (i + j) | l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + SUM_WRAPPED + "\n");

            // the all-INT peer agrees on the value, differing only in declared type
            assertQuery("SELECT (i + j) + 0 AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n" + SUM_WRAPPED + "\n");

            // widening an operand of the INNER arithmetic is what moves it to 64 bits
            assertQuery("SELECT (secs * 1_000_000L) + l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n1720468802000000\n");
        });
    }

    @Test
    public void testSampleByIntervalUsesWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sb (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO sb VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0),
                        ('2024-01-01T00:05:00.000000Z', 2.0),
                        ('2024-01-01T01:00:00.000000Z', 4.0),
                        ('2024-01-01T12:00:00.000000Z', 8.0)""");

            assertSqlCursors(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 500_000_000 + 654_080 U",
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000 U"
            );
            assertSqlCursors(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 500_000_000 + 654_080 U",
                    "SELECT ts, sum(v) FROM sb SAMPLE BY (86_400 * 1_000_000)::long U"
            );

            assertQuery("SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000L U")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t15.0
                            """);
        });
    }

    @Test
    public void testWindowFrameBoundUsesWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wf (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wf VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0),
                        ('2024-01-01T01:00:00.000000Z', 2.0),
                        ('2024-01-01T02:00:00.000000Z', 4.0),
                        ('2024-01-01T12:00:00.000000Z', 8.0)""");

            assertSqlCursors(
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 500_654_080 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 86_400 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf"
            );
        });
    }

    @Test
    public void testWindowJoinDynamicFrameBoundUsesWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wjdm (ts TIMESTAMP, k INT, wrapped INT, h INT, h2 INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjdm VALUES
                        ('2024-01-01T00:00:00.000000Z', 86_400, 500_654_080, 3_600, 7_200),
                        ('2024-01-01T01:00:00.000000Z', 86_400, 500_654_080, 3_600, 7_200),
                        ('2024-01-01T02:00:00.000000Z', 86_400, 500_654_080, 3_600, 7_200),
                        ('2024-01-01T12:00:00.000000Z', 86_400, 500_654_080, 3_600, 7_200)""");
            execute("CREATE TABLE wjds (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjds VALUES
                        ('2024-01-01T00:00:00.000000Z', 10.0),
                        ('2024-01-01T01:00:00.000000Z', 20.0),
                        ('2024-01-01T02:00:00.000000Z', 40.0),
                        ('2024-01-01T12:00:00.000000Z', 80.0)""");

            assertSqlCursors(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN wrapped PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """,
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN k * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """
            );
            assertSqlCursors(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN h2 SECOND PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """,
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN h * 2 SECOND PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """
            );
        });
    }

    @Test
    public void testWindowJoinFrameBoundUsesWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wjm (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wjm VALUES ('2024-01-01T00:00:00.000000Z'), ('2024-01-01T01:00:00.000000Z')");
            execute("CREATE TABLE wjs (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wjs VALUES ('2024-01-01T00:00:00.000000Z', 10.0), ('2024-01-01T01:00:00.000000Z', 20.0)");

            assertSqlCursors(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjm m
                            WINDOW JOIN wjs s RANGE BETWEEN 500_654_080 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """,
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjm m
                            WINDOW JOIN wjs s RANGE BETWEEN 86_400 * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts
                            """
            );
        });
    }
}
