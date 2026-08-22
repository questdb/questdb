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
 * The contexts that pull an INT expression into a 64-bit operation without an explicit cast asking
 * them to, where the width is decided by a PEER rather than by anything visible in the expression's
 * own syntax:
 * <ul>
 *     <li>arithmetic with a 64-bit peer. The LONG row of {@code ColumnType.OVERLOAD_PRIORITY}
 *     has no INT, so {@code +(II)} cannot match an {@code (INT, LONG)} pair, {@code +(LL)} wins
 *     and {@code AddLongFunctionFactory} reads the INT operand through {@code getLong()}.</li>
 *     <li>a conditional with a 64-bit arm. {@code CaseCommon.getCommonType} escalates
 *     {@code (INT, LONG)} to LONG and no {@code (INT, LONG)} cast factory exists, so the LONG
 *     variant of COALESCE / CASE / NULLIF reads the INT arm directly.</li>
 *     <li>a comparison against a function-valued 64-bit peer, whose declared type is known only
 *     once the peer has been built.</li>
 * </ul>
 * Each of them reads the INT expression through {@code getLong()}, and {@code IntFunction.getLong()}
 * is {@code Numbers.intToLong(getInt())}. So the peer decides the width of the OPERATION and never
 * the width of the operand: the INT arithmetic has already wrapped by the time the 64-bit function
 * sees it. That is what makes {@code SELECT expr} a reliable way to see what every consumer received.
 * <p>
 * Released 9.4.3 answered the full-width value in all three, because PR #4824 gave the INT
 * arithmetic operators a {@code getLong()} that recomputed at 64 bits. These are therefore
 * characterization tests of a deliberate divergence, not of released behaviour - see
 * {@link IntWidthWrapTest} for the rule and its cost.
 * <p>
 * Two contexts do NOT read the wrapped value, and they are the last four tests here: a window frame
 * width and a SAMPLE BY stride. Every context above reads a value the caller can also project and
 * inspect - {@code SELECT 86_400 * 1_000_000} answers 500654080 and says so. Neither of these two
 * has a projected spelling, so a wrapped product silently measures a frame, or buckets by an
 * interval, that nobody asked for and changes the aggregate on every row. Both readers therefore
 * refuse the wrap rather than acting on it, the way {@code ALTER TABLE ... DROP PARTITION WHERE}
 * already does.
 * <p>
 * A frame width has two spellings, and both reach a 64-bit read. A constant one is folded and read
 * by {@code SqlOptimiser}; a {@code WINDOW JOIN} bound that references a master column is compiled
 * by {@code SqlCodeGenerator} and read per master row by
 * {@code AsyncWindowJoinRecordCursorFactory.computeEffectiveBound()}. Both are guarded, because a
 * rule that refuses {@code 86_400 * 1_000_000} while silently narrowing {@code k * 1_000_000} over
 * an INT column holding 86_400 would leave the two spellings of one query disagreeing.
 * <p>
 * A SAMPLE BY stride has only one spelling that reaches arithmetic at all -
 * {@code SAMPLE BY <expr> <unit>} - and it is guarded at
 * {@code SqlCodeGenerator.generateSampleBy}. The bare-period spelling {@code SAMPLE BY 1d} parses
 * a token rather than compiling an expression, and a bare numeric constant followed by a unit is a
 * parse error, so there is no second reader to keep in step. The stride is the more invisible of
 * the two widths: {@code EXPLAIN} prints the same three lines for the eight-minute stride and the
 * one-day one.
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
    public void testSampleByIntervalRefusesWrappedIntArithmetic() throws Exception {
        // A SAMPLE BY stride is the second width with no projected spelling, and it is the more
        // invisible of the two. A window frame width at least has no plan line to contradict;
        // this one has a plan line that says nothing:
        //
        //   EXPLAIN ... SAMPLE BY 86_400 * 1_000_000 U   -> Sample By / fill: none / values: [sum(v)]
        //   EXPLAIN ... SAMPLE BY 86_400 * 1_000_000L U  -> Sample By / fill: none / values: [sum(v)]
        //
        // The two plans are identical while the two queries bucket by eight minutes and by one
        // day. So neither the projection, nor EXPLAIN, nor an equivalent SELECT can show the
        // operator which width the engine used - only the row timestamps, which look plausible
        // because they are still in the right era. That is why this reader refuses the wrap
        // instead of acting on it, exactly as the window frame bound does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sb (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO sb VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0),
                        ('2024-01-01T00:05:00.000000Z', 2.0),
                        ('2024-01-01T01:00:00.000000Z', 4.0),
                        ('2024-01-01T12:00:00.000000Z', 8.0)""");

            // one day as a micros conversion. 86_400 * 1_000_000 wraps to a POSITIVE 500_654_080,
            // so nothing downstream rejects it and the query silently buckets by 8m20.654s
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000 U",
                    43,
                    "INT arithmetic overflow in SAMPLE BY interval: this computes at 32 bits and wraps to 500654080 instead of 86400000000"
            );
            // one hour: the same conversion wraps NEGATIVE, and a negative stride is not refused
            // downstream either - it produces buckets just as confidently
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 3_600 * 1_000_000 U",
                    42,
                    "INT arithmetic overflow in SAMPLE BY interval: this computes at 32 bits and wraps to -694967296 instead of 3600000000"
            );
            // the wrap can sit at the outer node of a nested product
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 60 * 60 * 1_000_000 U",
                    44,
                    "wraps to -694967296 instead of 3600000000"
            );
            // landing exactly on INT's null sentinel used to reach TimestampSamplerFactory as
            // LONG_NULL and bucket every row into 1970
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 2_147_483_647 + 1 U",
                    50,
                    "wraps to -2147483648 instead of 2147483648"
            );
            // a cast on the OUTSIDE widens nothing: the INT product has already wrapped by the
            // time ::long reads it, so the guard judges the arithmetic where the arithmetic sits
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY (86_400 * 1_000_000)::long U",
                    44,
                    "wraps to 500654080 instead of 86400000000"
            );
            // the unit only scales the wrapped number, so every unit is reachable
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000 s",
                    43,
                    "wraps to 500654080 instead of 86400000000"
            );
            // a DECLARE substitutes the expression into the same AST, so it is the same node
            assertExceptionNoLeakCheck(
                    "DECLARE @p := 86_400 * 1_000_000 SELECT ts, sum(v) FROM sb SAMPLE BY @p U",
                    21,
                    "wraps to 500654080 instead of 86400000000"
            );
            // the refusal names the two remedies, and both of them are spellings SAMPLE BY
            // actually accepts - "write it as a single literal" is NOT one of them, because
            // SAMPLE BY 86400000000 U is a parse error ("unexpected token [U]")
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000 U",
                    43,
                    "widen an operand (1_000_000L, expr::long) or write the interval with a unit suffix (1d, 24h)"
            );

            final String wholeDay = """
                    ts\tsum
                    2024-01-01T00:00:00.000000Z\t15.0
                    """;
            // the unit-suffix spelling of the same stride is exact, and keeps working
            assertQuery("SELECT ts, sum(v) FROM sb SAMPLE BY 1d")
                    .noLeakCheck().timestamp("ts").sizeMayVary().returns(wholeDay);
            // widening either operand is the documented fix, and it is what the refusal names.
            // This is also the value the fold produced before this PR, when the INT arithmetic
            // factories carried a 64-bit getLong() and FunctionParser folded an overflowing INT
            // expression to a LongConstant - so this row is what SAMPLE BY 86_400 * 1_000_000 U
            // used to answer.
            assertQuery("SELECT ts, sum(v) FROM sb SAMPLE BY 86_400 * 1_000_000L U")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(wholeDay);
            assertQuery("SELECT ts, sum(v) FROM sb SAMPLE BY 86_400::long * 1_000_000 U")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(wholeDay);

            // arithmetic that stays inside the INT range carries the value the user wrote, so it
            // is untouched: a five-minute stride splits the first hour and keeps the rest apart
            assertQuery("SELECT ts, sum(v) FROM sb SAMPLE BY 5 * 60 s")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T00:05:00.000000Z\t2.0
                            2024-01-01T01:00:00.000000Z\t4.0
                            2024-01-01T12:00:00.000000Z\t8.0
                            """);

            // the two spellings the stride reader already refuses stay refused with THEIR message,
            // not with the overflow one: the guard runs after the constant check, so a stride the
            // engine cannot fold at all is still reported as unfoldable
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY $1 * 1_000_000 U",
                    39,
                    "sample by period must be a constant expression of INT or LONG type"
            );
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) FROM sb SAMPLE BY now()::int * 1_000_000 U",
                    47,
                    "sample by period must be a constant expression of INT or LONG type"
            );
        });
    }

    @Test
    public void testWindowFrameBoundRefusesWrappedIntArithmetic() throws Exception {
        // A window frame bound is the one 64-bit constant reader whose value the writer of the
        // query cannot see. SqlOptimiser reads it with getLong() off the folded constant, so an
        // INT product that wrapped is read as the width the user did NOT write, and there is no
        // SELECT spelling of a frame width to show them what the engine used. So this reader
        // refuses the wrap instead of acting on it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wf (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wf VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0),
                        ('2024-01-01T01:00:00.000000Z', 2.0),
                        ('2024-01-01T02:00:00.000000Z', 4.0),
                        ('2024-01-01T12:00:00.000000Z', 8.0)""");

            // one day as a micros conversion. 86_400 * 1_000_000 wraps to a POSITIVE 500_654_080,
            // so the reader's non-negative guard passes it and the frame silently shrinks from a
            // day to eight minutes - every row of the result is then a different aggregate.
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 86_400 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    57,
                    "INT arithmetic overflow in window frame bound: this computes at 32 bits and wraps to 500654080 instead of 86400000000"
            );
            // one hour: the same conversion wraps NEGATIVE, and the reader used to report the
            // sign of the wrapped value rather than the wrap that produced it
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 3_600 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    56,
                    "INT arithmetic overflow in window frame bound: this computes at 32 bits and wraps to -694967296 instead of 3600000000"
            );
            // the wrap can sit at the outer node of a nested product, which is only reachable
            // once the inner one has been proven exact
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 60 * 60 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    58,
                    "wraps to -694967296 instead of 3600000000"
            );
            // a ROWS frame counts rows rather than micros, and wraps the same way
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts ROWS BETWEEN 100_000 * 100_000 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    57,
                    "wraps to 1410065408 instead of 10000000000"
            );
            // landing exactly on INT's null sentinel used to reach the reader as a negative
            // width and be reported as a sign error; the wrap is the cause, and the cause is what
            // the refusal now names
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts ROWS BETWEEN 2_147_483_647 + 1 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    63,
                    "wraps to -2147483648 instead of 2147483648"
            );
            // the refusal names the remedy, which is the only place a caller learns what to do
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 86_400 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf",
                    57,
                    "widen an operand (1_000_000L, expr::long) or write the width as a single literal"
            );

            final String wholeDay = """
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t3.0
                    2024-01-01T02:00:00.000000Z\t7.0
                    2024-01-01T12:00:00.000000Z\t15.0
                    """;
            // the LONG literal spelling of the same bound is exact, and keeps working
            assertQuery("SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 86_400_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf")
                    .noLeakCheck().timestamp("ts").noRandomAccess().expectSize().returns(wholeDay);
            // widening an operand is the documented fix, and it is what the refusal names
            assertQuery("SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 86_400 * 1_000_000L PRECEDING AND CURRENT ROW) AS s FROM wf")
                    .noLeakCheck().timestamp("ts").noRandomAccess().expectSize().returns(wholeDay);

            // arithmetic that stays inside the INT range carries the value the user wrote, so it
            // is untouched: a one-minute frame admits only the current row here
            assertQuery("SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN 60 * 1_000_000 PRECEDING AND CURRENT ROW) AS s FROM wf")
                    .noLeakCheck().timestamp("ts").noRandomAccess().expectSize()
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:00:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t4.0
                            2024-01-01T12:00:00.000000Z\t8.0
                            """);
        });
    }

    @Test
    public void testWindowJoinDynamicFrameBoundRefusesWrappedIntArithmetic() throws Exception {
        // A WINDOW JOIN is the one frame whose bound may reference a master column, and such a
        // bound never reaches the constant reader at all: tryEvalNonNegativeLongConstant answers
        // "dynamic" the moment it sees a column, so SqlCodeGenerator compiles the expression
        // against the master metadata instead and AsyncWindowJoinRecordCursorFactory reads it per
        // master row with getLong(). That read narrows a wrapped INT product exactly as the
        // constant one did, so the two spellings of one frame - 86_400 * 1_000_000 and
        // k * 1_000_000 over an INT column holding 86_400 - have to answer the same way.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wjdm (ts TIMESTAMP, k INT, h INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjdm VALUES
                        ('2024-01-01T00:00:00.000000Z', 86_400, 3_600),
                        ('2024-01-01T01:00:00.000000Z', 86_400, 3_600),
                        ('2024-01-01T02:00:00.000000Z', 86_400, 3_600),
                        ('2024-01-01T12:00:00.000000Z', 86_400, 3_600)""");
            execute("CREATE TABLE wjds (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjds VALUES
                        ('2024-01-01T00:00:00.000000Z', 10.0),
                        ('2024-01-01T01:00:00.000000Z', 20.0),
                        ('2024-01-01T02:00:00.000000Z', 40.0),
                        ('2024-01-01T12:00:00.000000Z', 80.0)""");

            final String wholeDay = """
                    ts\ts
                    2024-01-01T00:00:00.000000Z\t10.0
                    2024-01-01T01:00:00.000000Z\t30.0
                    2024-01-01T02:00:00.000000Z\t70.0
                    2024-01-01T12:00:00.000000Z\t150.0
                    """;

            // the two spellings that carry the whole day exactly, and are what the refusal names
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjdm m
                    WINDOW JOIN wjds s RANGE BETWEEN 86_400_000_000 PRECEDING AND CURRENT ROW
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(wholeDay);
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjdm m
                    WINDOW JOIN wjds s RANGE BETWEEN k * 1_000_000L PRECEDING AND CURRENT ROW
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(wholeDay);

            // the INT arithmetic spelling of that same frame used to answer 10 / 30 / 60 / 120 -
            // 86_400 * 1_000_000 wraps to a positive 500654080, so the frame measured eight
            // minutes and every row of the result was a different aggregate
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN k * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    74,
                    "INT arithmetic overflow in window frame bound cannot be ruled out: this computes at 32 bits and an operand is only known once the statement runs"
            );
            // and it names the remedy the two working spellings above use
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN k * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    74,
                    "widen an operand (1_000_000L, expr::long) or write the width as a single literal"
            );
            // the negative wrap: 3_600 * 1_000_000 is -694967296, which computeEffectiveBound
            // clamps to zero, so the frame used to collapse to a single instant
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN h * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    74,
                    "INT arithmetic overflow in window frame bound cannot be ruled out"
            );
            // the hi bound is read by the same per-row call, so FOLLOWING is guarded too
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN CURRENT ROW AND h * 1_000_000 FOLLOWING
                            ORDER BY m.ts""",
                    90,
                    "INT arithmetic overflow in window frame bound cannot be ruled out"
            );
            // a constant sub-expression of a dynamic bound is still proven exactly, so the
            // refusal quotes both values rather than saying it cannot be ruled out
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN k + 86_400 * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    83,
                    "INT arithmetic overflow in window frame bound: this computes at 32 bits and wraps to 500654080 instead of 86400000000"
            );
            // Arithmetic over a column cannot be proven either way, so the guard fails closed and
            // refuses it whether or not it could ever wrap. That is the cost of the rule and it is
            // pinned here deliberately: h * 2 is 7200 for every row of this table, and it is
            // refused all the same. The remedy - h * 2L, or h::long * 2 - is the same one the
            // message names, and it is expressible for every dynamic bound.
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjdm m
                            WINDOW JOIN wjds s RANGE BETWEEN h * 2 SECOND PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    74,
                    "INT arithmetic overflow in window frame bound cannot be ruled out"
            );

            // a bare column bound carries no arithmetic, so it is never refused - k SECOND is the
            // same whole day the wrapped product was reaching for
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjdm m
                    WINDOW JOIN wjds s RANGE BETWEEN k SECOND PRECEDING AND CURRENT ROW
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(wholeDay);
            // and the widened hi bound keeps working, an hour of slave rows ahead of each master row
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjdm m
                    WINDOW JOIN wjds s RANGE BETWEEN CURRENT ROW AND h * 1_000_000L FOLLOWING
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t30.0
                            2024-01-01T01:00:00.000000Z\t60.0
                            2024-01-01T02:00:00.000000Z\t40.0
                            2024-01-01T12:00:00.000000Z\t80.0
                            """);
        });
    }

    @Test
    public void testWindowJoinFrameBoundRefusesWrappedIntArithmetic() throws Exception {
        // A WINDOW JOIN frame bound reaches the same reader through a different helper
        // (tryEvalNonNegativeLongConstant, which also has to answer "dynamic" for a column
        // reference), so it needs its own row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wjm (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjm VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0),
                        ('2024-01-01T01:00:00.000000Z', 2.0),
                        ('2024-01-01T02:00:00.000000Z', 4.0),
                        ('2024-01-01T12:00:00.000000Z', 8.0)""");
            execute("CREATE TABLE wjs (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO wjs VALUES
                        ('2024-01-01T00:00:00.000000Z', 10.0),
                        ('2024-01-01T01:00:00.000000Z', 20.0),
                        ('2024-01-01T02:00:00.000000Z', 40.0),
                        ('2024-01-01T12:00:00.000000Z', 80.0)""");

            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjm m
                            WINDOW JOIN wjs s RANGE BETWEEN 86_400 * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    77,
                    "INT arithmetic overflow in window frame bound: this computes at 32 bits and wraps to 500654080 instead of 86400000000"
            );
            assertExceptionNoLeakCheck(
                    """
                            SELECT m.ts, sum(s.x) AS s FROM wjm m
                            WINDOW JOIN wjs s RANGE BETWEEN 3_600 * 1_000_000 PRECEDING AND CURRENT ROW
                            ORDER BY m.ts""",
                    76,
                    "wraps to -694967296 instead of 3600000000"
            );

            // the exact LONG spelling of a one-day window, which the wrapped bound above silently
            // replaced with an eight-minute one
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjm m
                    WINDOW JOIN wjs s RANGE BETWEEN 86_400_000_000 PRECEDING AND CURRENT ROW
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t30.0
                            2024-01-01T02:00:00.000000Z\t70.0
                            2024-01-01T12:00:00.000000Z\t150.0
                            """);
            // in-range arithmetic keeps working
            assertQuery("""
                    SELECT m.ts, sum(s.x) AS s FROM wjm m
                    WINDOW JOIN wjs s RANGE BETWEEN 60 * 1_000_000 PRECEDING AND CURRENT ROW
                    ORDER BY m.ts""")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t30.0
                            2024-01-01T02:00:00.000000Z\t60.0
                            2024-01-01T12:00:00.000000Z\t120.0
                            """);
        });
    }
}
