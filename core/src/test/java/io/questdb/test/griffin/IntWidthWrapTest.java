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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.cast.CastIntToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToTimestampFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The rule: an INT expression carries exactly one value - the value its four bytes hold. INT
 * arithmetic wraps modulo 2^32 in every context, exactly as LONG arithmetic wraps modulo 2^64. To
 * compute at 64 bits, widen an operand: {@code secs * 1_000_000L}, or {@code i::long * j}.
 * <p>
 * This is the spelling matrix that pins it. One overflowing expression, written three ways - a
 * literal, INT column arithmetic, and bind-variable arithmetic - crossed with every context that
 * could plausibly read it at 64 bits. Every cell must show the wrap, so a future change to any of
 * them reddens deliberately rather than silently.
 * <p>
 * Two consequences are deliberate and were argued for rather than inherited:
 * <ul>
 *     <li>GitHub issue #4752 reopens in its reported form. {@code to_utc(1_720_468_802 *
 *     1_000_000, tz)} returns a 1970 date. The workaround the issue itself named -
 *     {@code 1_000_000L} - is the fix, and it is error free.</li>
 *     <li>Nullness stops depending on context. {@code 2147483647 + 1} lands on {@code INT_NULL}
 *     and reads as NULL everywhere, {@code ::long} included, because
 *     {@code Numbers.intToLong(INT_NULL) == LONG_NULL}.</li>
 * </ul>
 */
public class IntWidthWrapTest extends AbstractCairoTest {

    // 1_720_468_802 * 1_000_000 needs 51 bits, so it wraps to this.
    private static final String SECS_WRAPPED = "-607497088";
    // The timestamp the wrapped seconds-to-micros product renders as.
    private static final String SECS_WRAPPED_TS = "1969-12-31T23:49:52.502912Z";
    // 2_000_000_000 + 2_000_000_000 needs 32 bits, so it wraps to this.
    private static final String SUM_WRAPPED = "-294967296";

    @Test
    public void testAliasReadsTheSameValueAsTheExpression() throws Exception {
        // A projection that references an earlier column by name creates a column function over a
        // 4-byte slot. With one value per expression there is nothing for it to lose, so the alias
        // and the un-aliased spelling agree - and so does a filter compiled over the alias, which
        // resolves the same column reference.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE al (i INT, j INT)");
            execute("INSERT INTO al VALUES (2_000_000_000, 2_000_000_000)");

            assertQuery("SELECT i + j AS a, a::long AS b, (i + j)::long AS c FROM al")
                    .noLeakCheck().expectSize()
                    .columnType(0, ColumnType.INT)
                    .columnType(1, ColumnType.LONG)
                    .columnType(2, ColumnType.LONG)
                    .returns("a\tb\tc\n" + SUM_WRAPPED + "\t" + SUM_WRAPPED + "\t" + SUM_WRAPPED + "\n");

            // the filter reads the alias at the same width the projection emitted it
            assertQuery("SELECT a FROM (SELECT i + j AS a, i FROM al) WHERE a = " + SUM_WRAPPED)
                    .noLeakCheck().sizeMayVary().returns("a\n" + SUM_WRAPPED + "\n");
            assertQuery("SELECT count() AS c FROM (SELECT i + j AS a, i FROM al) WHERE a = 4000000000")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    @Test
    public void testCastsReadTheWrappedValue() throws Exception {
        // The three 64-bit casts and the floating-point ones all read the same four bytes. Before
        // this change ::LONG / ::TIMESTAMP / ::DATE reached a wider value that ::DOUBLE could not,
        // so an expression's value depended on which cast was asked for.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ca (secs INT, i INT, j INT)");
            execute("INSERT INTO ca VALUES (1_720_468_802, 2_000_000_000, 2_000_000_000)");

            assertQuery("""
                    SELECT (i + j)::long AS l, (i + j)::double AS d, (i + j)::float AS f
                    FROM ca""")
                    .noLeakCheck().expectSize()
                    .returns("l\td\tf\n" + SUM_WRAPPED + "\t-2.94967296E8\t-2.94967296E8\n");

            assertQuery("SELECT (secs * 1_000_000)::timestamp AS t, (secs * 1_000_000)::long AS l FROM ca")
                    .noLeakCheck().expectSize()
                    .returns("t\tl\n" + SECS_WRAPPED_TS + "\t" + SECS_WRAPPED + "\n");

            // widening an operand is the way to compute at 64 bits, and it is the documented fix
            assertQuery("SELECT (secs * 1_000_000L)::timestamp AS t FROM ca")
                    .noLeakCheck().expectSize().returns("t\n2024-07-08T20:00:02.000000Z\n");
            assertQuery("SELECT (secs::long * 1_000_000)::timestamp AS t FROM ca")
                    .noLeakCheck().expectSize().returns("t\n2024-07-08T20:00:02.000000Z\n");
        });
    }

    @Test
    public void testTemporalCastFactoriesReadIntGetterDirectly() {
        Function arg = new IntFunction() {
            @Override
            public int getInt(Record rec) {
                return 7;
            }

            @Override
            public long getLong(Record rec) {
                return 99;
            }
        };

        Assert.assertEquals(7, new CastIntToDateFunctionFactory.CastIntToDateFunction(arg).getDate(null));
        Assert.assertEquals(
                7,
                new CastIntToTimestampFunctionFactory.Func(arg, ColumnType.TIMESTAMP).getTimestamp(null)
        );
    }

    @Test
    public void testComparisonAgainstWideBoundReadsTheWrappedValue() throws Exception {
        // A comparison peer cannot change the arithmetic's width, so the predicate is true at the
        // wrapped value and false at the mathematical one, whichever bound it is written against.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cm (secs INT, i INT, j INT, l LONG)");
            execute("INSERT INTO cm VALUES (1_720_468_802, 2_000_000_000, 2_000_000_000, 0)");

            assertQuery("SELECT count() AS c FROM cm WHERE secs * 1_000_000 > 1_000_000_000_000")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
            assertQuery("SELECT count() AS c FROM cm WHERE secs * 1_000_000L > 1_000_000_000_000")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            // a bound the wrap does clear, so the predicate is not vacuously false
            assertQuery("SELECT count() AS c FROM cm WHERE secs * 1_000_000 > -1_000_000_000")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            // an INT-range bound and a LONG-range bound agree about the same row: a predicate must
            // not be true at the larger bound and false at the smaller one
            assertQuery("SELECT count() AS c FROM cm WHERE i + j > 2147483647")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
            assertQuery("SELECT count() AS c FROM cm WHERE i + j > 2147483648")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    @Test
    public void testCtasAndInsertStoreTheWrappedValue() throws Exception {
        // The store path is purely type-directed again: an INT source column reads getInt() for
        // every target, so the row keeps what an explicit cast of the same expression reads.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (secs INT, i INT, j INT)");
            execute("INSERT INTO src VALUES (1_720_468_802, 2_000_000_000, 2_000_000_000)");

            // CTAS keeps the INT type and stores the wrap
            execute("CREATE TABLE ctas AS (SELECT i + j AS v FROM src)");
            assertQuery("SELECT v FROM ctas").noLeakCheck().expectSize()
                    .columnType(0, ColumnType.INT).returns("v\n" + SUM_WRAPPED + "\n");

            // a constant fold is an IntConstant, so its CTAS column is INT too
            execute("CREATE TABLE ctasConst AS (SELECT 1_000_000 * 1_000_000 AS v FROM long_sequence(1))");
            assertQuery("SELECT v FROM ctasConst").noLeakCheck().expectSize()
                    .columnType(0, ColumnType.INT).returns("v\n-727379968\n");

            // INSERT ... SELECT into 64-bit targets stores the wrap, matching the cast
            execute("CREATE TABLE dst (l LONG, t TIMESTAMP, d DATE)");
            execute("INSERT INTO dst SELECT i + j, secs * 1_000_000, secs * 1_000_000 FROM src");
            assertQuery("SELECT l, t, d FROM dst").noLeakCheck().expectSize()
                    .returns("l\tt\td\n" + SUM_WRAPPED + "\t" + SECS_WRAPPED_TS + "\t1969-12-24T23:15:02.912Z\n");

            // INSERT ... VALUES reaches the writer without a copier and must agree
            execute("CREATE TABLE dstVals (l LONG, t TIMESTAMP)");
            execute("INSERT INTO dstVals VALUES (2_000_000_000 + 2_000_000_000, 1_720_468_802 * 1_000_000)");
            assertQuery("SELECT l, t FROM dstVals").noLeakCheck().expectSize()
                    .returns("l\tt\n" + SUM_WRAPPED + "\t" + SECS_WRAPPED_TS + "\n");
        });
    }

    @Test
    public void testDesignatedTimestampTargetRejectsTheWrappedValue() throws Exception {
        // The designated timestamp is the one store target the row copier skips, and it is also the
        // one target that validates its input: a value before the epoch is refused outright. The
        // wrapped seconds-to-micros product is negative, so EVERY route into a designated timestamp
        // now errors rather than storing a 1970 date - the bare INSERT ... SELECT on the type check,
        // and both the cast and the VALUES form on the bounds check.
        //
        // Released 9.4.3 stored a valid 2024 timestamp through the latter two. This is the loudest
        // divergence the change produces, and the better failure mode of the two available, but it
        // IS a behaviour change: an ingest that relied on the widening now errors instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dtsSrc (secs INT)");
            execute("INSERT INTO dtsSrc VALUES (1_720_468_802)");
            execute("CREATE TABLE dts (t TIMESTAMP) TIMESTAMP(t) PARTITION BY YEAR");

            // rejected before any copier is generated; the guard is identical in released 9.4.3
            assertExceptionNoLeakCheck(
                    "INSERT INTO dts SELECT secs * 1_000_000 FROM dtsSrc",
                    12,
                    "expected timestamp column but type is INT"
            );
            // the cast is accepted by the compiler and rejected by the writer
            assertExceptionNoLeakCheck(
                    "INSERT INTO dts SELECT (secs * 1_000_000)::TIMESTAMP FROM dtsSrc",
                    -1,
                    "designated timestamp before 1970-01-01 is not allowed"
            );
            // VALUES reaches the writer through InsertRowImpl's timestampFunction.getTimestamp(null)
            assertExceptionNoLeakCheck(
                    "INSERT INTO dts VALUES (1_720_468_802 * 1_000_000)",
                    -1,
                    "designated timestamp before 1970-01-01 is not allowed"
            );

            // widening an operand restores both, and is the documented fix
            execute("INSERT INTO dts SELECT (secs * 1_000_000L)::TIMESTAMP FROM dtsSrc");
            execute("INSERT INTO dts VALUES (1_720_468_802 * 1_000_000L)");
            assertQuery("SELECT t FROM dts").noLeakCheck().timestamp("t").expectSize()
                    .returns("t\n2024-07-08T20:00:02.000000Z\n2024-07-08T20:00:02.000000Z\n");

            // a wrap that lands positive still stores, so the rejection is about the sign of the
            // wrapped value and not about the wrap itself: 1_500_000_000 * 3 wraps to 205_032_704
            execute("CREATE TABLE dtsOk (t TIMESTAMP) TIMESTAMP(t) PARTITION BY YEAR");
            execute("INSERT INTO dtsOk VALUES (1_500_000_000 * 3)");
            assertQuery("SELECT t FROM dtsOk").noLeakCheck().timestamp("t").expectSize()
                    .returns("t\n1970-01-01T00:03:25.032704Z\n");
        });
    }

    @Test
    public void testNullSentinelReadsAsNullInEveryContext() throws Exception {
        // INT's null sentinel is -2^31, and an INT expression that lands on it IS null - at every
        // width, because Numbers.intToLong(INT_NULL) == LONG_NULL. Under the two-value regime the
        // same expression read as NULL at INT width and as an ordinary -2147483648 at 64 bits, so
        // coalesce() picked a different branch depending on the cast around it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE se (a INT, b INT, c INT)");
            execute("INSERT INTO se VALUES (2147483647, 1, -1073741824)");

            // three ways to land exactly on INT_NULL: overflow wrap, a genuine -2^31, and ~MAX
            assertQuery("""
                    SELECT (a + b) AS s, (a + b)::long AS sl, (c * 2) AS m, (c * 2)::long AS ml,
                           (~a) AS n, (~a)::long AS nl
                    FROM se""")
                    .noLeakCheck().expectSize()
                    .returns("s\tsl\tm\tml\tn\tnl\nnull\tnull\tnull\tnull\tnull\tnull\n");

            // and the branch a conditional picks does not depend on the cast around it
            assertQuery("""
                    SELECT coalesce(a + b, 42) AS x, coalesce(a + b, 42)::long AS y,
                           coalesce(c * 2, 42) AS z
                    FROM se""")
                    .noLeakCheck().expectSize().returns("x\ty\tz\n42\t42\t42\n");

            assertQuery("SELECT count() AS c FROM se WHERE (a + b) IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() AS c FROM se WHERE (a + b)::long IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
        });
    }

    @Test
    public void testTemporalFunctionsReadTheWrappedValue() throws Exception {
        // GitHub issue #4752 in its reported form. This is the cost of the rule, stated plainly:
        // the seconds-to-micros conversion returns a 1970 date again. The workaround the issue
        // itself named is the fix, and it is asserted alongside so the two sit next to each other.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (secs INT)");
            execute("INSERT INTO tz VALUES (1_720_468_802)");
            execute("CREATE TABLE tzt (ts TIMESTAMP, stride INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tzt VALUES ('2024-01-01T00:00:00.000000Z', 3_600)");

            assertQuery("SELECT to_utc(1_720_468_802 * 1_000_000, 'Europe/Berlin') AS v FROM tz")
                    .noLeakCheck().expectSize().returns("v\n1969-12-31T22:49:52.502912Z\n");
            assertQuery("SELECT to_utc(secs * 1_000_000, 'Europe/Berlin') AS v FROM tz")
                    .noLeakCheck().expectSize().returns("v\n1969-12-31T22:49:52.502912Z\n");

            // widen an operand and the conversion is correct again
            assertQuery("SELECT to_utc(secs * 1_000_000L, 'Europe/Berlin') AS v FROM tz")
                    .noLeakCheck().expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");

            // A dateadd() stride is INT-only, so the constant spelling now resolves the same
            // overload the column spelling always did, and both read the same wrapped stride.
            // Under the two-value regime the constant folded to a LONG and dateadd had no
            // (CHAR, LONG, TIMESTAMP) overload at all, so the two spellings of one expression
            // disagreed on whether the statement even compiled.
            assertQuery("SELECT dateadd('u', 3_600 * 1_000_000, ts) AS v FROM tzt")
                    .noLeakCheck().expectSize().returns("v\n2023-12-31T23:48:25.032704Z\n");
            assertQuery("SELECT dateadd('u', stride * 1_000_000, ts) AS v FROM tzt")
                    .noLeakCheck().expectSize().returns("v\n2023-12-31T23:48:25.032704Z\n");
            // widening moves the stride out of dateadd's reach, which is the loud failure mode
            assertExceptionNoLeakCheck(
                    "SELECT dateadd('u', 3_600 * 1_000_000L, ts) AS v FROM tzt",
                    7,
                    "no matching function"
            );
        });
    }

    @Test
    public void testTimestampBoundsUseTheWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tb (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tb VALUES ('2024-01-01T00:00:00.000000Z', 1)");

            assertSqlCursors(
                    "SELECT count() AS c FROM tb WHERE ts > -607_497_088",
                    "SELECT count() AS c FROM tb WHERE ts > 1_720_468_802 * 1_000_000"
            );
            assertSqlCursors(
                    "SELECT count() AS c FROM tb WHERE ts > 2",
                    "SELECT count() AS c FROM tb WHERE ts > 1 + 1"
            );
            assertSqlCursors(
                    "SELECT ts, sum(v) FROM tb SAMPLE BY 1d FROM -694_967_296",
                    "SELECT ts, sum(v) FROM tb SAMPLE BY 1d FROM 3_600 * 1_000_000"
            );
            assertSqlCursors(
                    "SELECT count() AS c FROM tb WHERE ts IN (2)",
                    "SELECT count() AS c FROM tb WHERE ts IN (1 + 1)"
            );

            assertQuery("SELECT count() AS c FROM tb WHERE ts > 1_720_468_802 * 1_000_000L")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    @Test
    public void testThreeSpellingsAgree() throws Exception {
        // The literal, column and bind-variable spellings of one expression are the same
        // expression, so they must carry the same value. The literal form used to fold to a
        // LongConstant and keep the full magnitude while the other two wrapped - the asymmetry the
        // query fuzzer's literal-vs-bind oracle needed a tolerance for.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sp (i INT, j INT)");
            execute("INSERT INTO sp VALUES (1_000_000, 1_000_000)");

            final String expected = "v\tw\n-727379968\t-727379968\n";
            assertQuery("SELECT 1_000_000 * 1_000_000 AS v, (1_000_000 * 1_000_000)::long AS w FROM sp")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns(expected);
            assertQuery("SELECT i * j AS v, (i * j)::long AS w FROM sp")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns(expected);

            bindVariableService.clear();
            bindVariableService.setInt(0, 1_000_000);
            assertQuery("SELECT i * $1 AS v, (i * $1)::long AS w FROM sp")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns(expected);
        });
    }

    @Test
    public void testUpdateIntoWiderColumnStoresTheWrappedValue() throws Exception {
        // An UPDATE SET target is a typed destination that bypasses the row copier:
        // UpdateOperatorImpl.updateColumnValues reads the virtual record with getLong() /
        // getTimestamp() / getDate() dispatched on the TARGET column's type. All three now
        // sign-extend the wrapped INT, so the stored value matches the projected one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE upd (l LONG, t TIMESTAMP, d DATE, i INT, j INT)");
            execute("INSERT INTO upd VALUES (0, '1970-01-01T00:00:00.000000Z', '1970-01-01T00:00:00.000Z', 2_000_000_000, 2_000_000_000)");

            execute("UPDATE upd SET l = i + j");
            assertQuery("SELECT l FROM upd").noLeakCheck().expectSize().returns("l\n" + SUM_WRAPPED + "\n");

            execute("UPDATE upd SET t = i + j");
            assertQuery("SELECT t FROM upd").noLeakCheck().expectSize().returns("t\n1969-12-31T23:55:05.032704Z\n");

            execute("UPDATE upd SET d = i + j");
            assertQuery("SELECT d FROM upd").noLeakCheck().expectSize().returns("d\n1969-12-28T14:03:52.704Z\n");

            // a real stored INT column keeps its INT-width read, NULL included
            execute("CREATE TABLE updi (l LONG, i INT)");
            execute("INSERT INTO updi VALUES (0, -2_147_483_648), (0, 7)");
            execute("UPDATE updi SET l = i");
            assertQuery("SELECT l FROM updi").noLeakCheck().expectSize().returns("l\nnull\n7\n");
        });
    }
}
