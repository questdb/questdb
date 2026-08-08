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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The two places a bounded RANGE frame overflows: converting the width the user wrote, and
 * measuring the distance between two rows at runtime.
 * <p>
 * A RANGE frame bound carries a time unit, and the runtime converts it into the designated
 * timestamp's own units before it builds the frame. That conversion multiplies by a per-unit
 * constant without checking the result, so a width the units cannot carry used to come back as
 * a different width - and the query then computed over a frame nobody wrote, silently.
 * <p>
 * These tests cover the three ways that went wrong, one per shape at each end of the frame, on
 * both timestamp resolutions, and pin the widest width that must keep compiling at each end.
 * The widths involved are far outside real time-series use; what makes them worth a test is
 * that two of the three failed without a word.
 * <p>
 * The runtime side is the {@code testABoundedRangeFrame...} pair. Every bounded RANGE family
 * decides what has left the frame by measuring the current row against a buffered one, and it
 * measured with {@code Math.abs(timestamp - ts)} - which wraps once the two rows straddle zero
 * far enough apart, because {@code Math.abs(Long.MIN_VALUE)} is itself negative. The furthest
 * apart two rows can be then reads as the closest possible pair, and the row the frame has
 * passed stays in the aggregate. A table cannot reach it - {@code TimestampDriver.validateBounds}
 * confines a designated timestamp to 1970..9999 - but {@code generate_series} is a designated
 * timestamp source that bounds-checks nothing, so an ordinary SELECT can.
 */
public class WindowRangeFrameOverflowTest extends AbstractCairoTest {

    // Five rows at -2^62, -2^61, 0, 2^61 and 2^62. The span between the first and the last is
    // exactly 2^63, the one distance whose subtraction lands on Long.MIN_VALUE, and every
    // adjacent pair is far enough apart that the frame below admits some rows and not others.
    private static final String SERIES =
            "(SELECT generate_series ts, 1 k, generate_series::long x" +
                    " FROM generate_series((-4611686018427387904)::timestamp, 4611686018427387904::timestamp, 2305843009213693952L))";
    // 7e18 reaches back over three of the four steps but not over all four, so the last row's
    // frame must drop the first row and keep the other three.
    private static final String WINDOW = " RANGE BETWEEN 7000000000000000000 PRECEDING AND CURRENT ROW)";
    // The lagging shape, whose low bound is unbounded and whose high bound is what each row is
    // measured against. 8e18 is wider than the three steps between the first row and the fourth,
    // so the first row does not reach any frame until the last one - where the distance is the
    // 2^63 that overflows. Every other case here measures against the frame's low bound; this is
    // the one that measures against its high bound.
    private static final String LAGGING_WINDOW = " RANGE BETWEEN UNBOUNDED PRECEDING AND 8000000000000000000 PRECEDING)";

    @Test
    public void testABoundedRangeFrameDropsTheRowItPassedAcrossTheWholeLongRange() throws Exception {
        assertMemoryLeak(() -> {
            // Unpartitioned first: no map, no group, and the class the partitioned spelling
            // inherits its frame arithmetic from. Then the partitioned one, which is what the
            // window Map group fuses. Both measured the same way, so both were wrong.
            for (String over : new String[]{"(ORDER BY ts", "(PARTITION BY k ORDER BY ts"}) {
                assertQuery(
                        "SELECT ts::long v, count(*) OVER w c, sum(x) OVER w s, min(x) OVER w mn, first_value(x) OVER w fv" +
                                " FROM " + SERIES + " WINDOW w AS " + over + WINDOW
                )
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                v\tc\ts\tmn\tfv
                                -4611686018427387904\t1\t-4.611686018427388E18\t-4611686018427387904\t-4611686018427387904
                                -2305843009213693952\t2\t-6.917529027641082E18\t-4611686018427387904\t-4611686018427387904
                                0\t3\t-6.917529027641082E18\t-4611686018427387904\t-4611686018427387904
                                2305843009213693952\t4\t-4.611686018427388E18\t-4611686018427387904\t-4611686018427387904
                                4611686018427387904\t4\t4.611686018427388E18\t-2305843009213693952\t-2305843009213693952
                                """);
            }
        });
    }

    @Test
    public void testABoundedRangeFrameDropsTheRowItPassedOnBothCachedCursors() throws Exception {
        assertMemoryLeak(() -> {
            // The same frame on the cached cursors, where the rows reach the functions through a
            // sorted record chain rather than off the base cursor. Both cached factories and both
            // fusion modes, because the physical plan a query lands on must not decide what the
            // frame answers. The leading avg is only there to force a cached cursor; its own
            // partition spans every row, and they sum to zero.
            final String sql = "SELECT ts::long v, avg(x) OVER (PARTITION BY k) forced," +
                    " count(*) OVER w c, sum(x) OVER w s, min(x) OVER w mn, first_value(x) OVER w fv" +
                    " FROM " + SERIES + " WINDOW w AS (PARTITION BY k ORDER BY ts" + WINDOW;
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                for (int fusion = 0; fusion < 2; fusion++) {
                    setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, fusion == 0 ? "true" : "false");
                    assertQuery(sql)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    v\tforced\tc\ts\tmn\tfv
                                    -4611686018427387904\t0.0\t1\t-4.611686018427388E18\t-4611686018427387904\t-4611686018427387904
                                    -2305843009213693952\t0.0\t2\t-6.917529027641082E18\t-4611686018427387904\t-4611686018427387904
                                    0\t0.0\t3\t-6.917529027641082E18\t-4611686018427387904\t-4611686018427387904
                                    2305843009213693952\t0.0\t4\t-4.611686018427388E18\t-4611686018427387904\t-4611686018427387904
                                    4611686018427387904\t0.0\t4\t4.611686018427388E18\t-2305843009213693952\t-2305843009213693952
                                    """);
                }
            }
        });
    }

    @Test
    public void testALaggingRangeFrameAdmitsTheRowItReachesAcrossTheWholeLongRange() throws Exception {
        assertMemoryLeak(() -> {
            // The mirror of the case above, on the other bound. Here the overflow drops a row
            // the frame should have admitted rather than keeping one it should have dropped:
            // the wrapped distance is negative, so it fails the "far enough behind" test and
            // the loop stops before the row is ever added.
            for (String over : new String[]{"(ORDER BY ts", "(PARTITION BY k ORDER BY ts"}) {
                assertQuery(
                        "SELECT ts::long v, count(*) OVER w c, sum(x) OVER w s, first_value(x) OVER w fv" +
                                " FROM " + SERIES + " WINDOW w AS " + over + LAGGING_WINDOW
                )
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                v\tc\ts\tfv
                                -4611686018427387904\t0\tnull\tnull
                                -2305843009213693952\t0\tnull\tnull
                                0\t0\tnull\tnull
                                2305843009213693952\t0\tnull\tnull
                                4611686018427387904\t1\t-4.611686018427388E18\t-4611686018427387904
                                """);
            }
        });
    }

    @Test
    public void testFrameEndNarrowingToZeroOnMicrosIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // The conversion narrows hours to int before it scales them, so this width used to
            // truncate to exactly 0: the frame end silently became CURRENT ROW and the query ran
            // a running total rather than the lagging window asked for. On micros the narrowing
            // caps hours long before the multiply does, so the ceiling names 2^31 - 1 hours.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 4294967296 HOUR PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=4294967296 hour, max=2147483647 hour]");
        });
    }

    @Test
    public void testFrameEndOutOfRangeIsNamedAtItsOwnPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // The high bound runs through the same conversion, and its error names the end of
            // the frame rather than the start.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 300000 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=300000 day, max=106751 day]");
        });
    }

    @Test
    public void testFrameEndWrappingToNegativeOnMicrosIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // Seconds reach micros through a plain multiply, so 20 trillion of them wrapped onto
            // a negative value that reads as a legal lagging end bound of the wrong magnitude -
            // about 49 thousand years rather than the 634 thousand asked for. Nothing caught this
            // one: the query returned rows over the narrower frame.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 20000000000000 SECOND PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=20000000000000 second, max=9223372036854 second]");
        });
    }

    @Test
    public void testFrameEndWrappingToPositiveOnMicrosIsNamedAsAWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // One second past the ceiling wraps onto a positive value, which frame validation
            // then turned away as a FOLLOWING frame end - the right refusal for a cause the user
            // did not write. The width is now named instead.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 9223372036855 SECOND PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=9223372036855 second, max=9223372036854 second]");
        });
    }

    @Test
    public void testFrameStartNarrowingToZeroIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // The conversion narrows days to int, so this width used to truncate to exactly 0:
            // the frame silently became CURRENT ROW and every window read one row.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 4294967296 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .fails(50, "RANGE frame start is out of range for the designated timestamp [width=4294967296 day, max=106751 day]");
        });
    }

    @Test
    public void testFrameStartWrappingToNegativeIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // 300000 days of nanoseconds wrapped onto a negative value, which reads as a legal
            // width of the wrong magnitude - about 236 years rather than the 821 asked for.
            // Nothing caught this one: the query returned rows over the narrower frame.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 300000 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .fails(50, "RANGE frame start is out of range for the designated timestamp [width=300000 day, max=106751 day]");
        });
    }

    @Test
    public void testFrameStartWrappingToPositiveIsNamedAsAWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // 200000 days of nanoseconds wrapped onto a positive value, which frame validation
            // then turned away as a FOLLOWING frame start - the right refusal for a cause the
            // user did not write. The width is now named instead.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 200000 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .fails(50, "RANGE frame start is out of range for the designated timestamp [width=200000 day, max=106751 day]");
        });
    }

    @Test
    public void testWidestMicrosFrameCompilesAndTheNextOneDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tab VALUES ('1970-01-01T00:00:00.000000Z', 1), ('2261-01-01T00:00:00.000000Z', 2)");

            // Micros carry three orders of magnitude more days than nanos do, so the ceiling
            // has to follow the column's resolution rather than a fixed magnitude.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 106751991 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000Z\t1.0
                            2261-01-01T00:00:00.000000Z\t3.0
                            """);
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 106751992 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .fails(50, "RANGE frame start is out of range for the designated timestamp [width=106751992 day, max=106751991 day]");
        });
    }

    @Test
    public void testWidestMicrosFrameEndCompilesAndTheNextOneDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tab VALUES ('1970-01-01T00:00:00.000000Z', 1), ('9999-01-01T00:00:00.000000Z', 2)");

            // 300000 days - 821 years - is a lagging end bound micros carry and nanos do not, so
            // the ceiling has to follow the column's resolution at this end of the frame too. The
            // 9999 row's frame stops in the year 9177 and still reaches the 1970 row; the 1970
            // row's frame stops before anything was written.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 300000 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000Z\tnull
                            9999-01-01T00:00:00.000000Z\t1.0
                            """);
            // The widest end bound micros carry reaches back past every row, so both frames are
            // empty - the point is that it compiles at all.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 106751991 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000Z\tnull
                            9999-01-01T00:00:00.000000Z\tnull
                            """);
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 106751992 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=106751992 day, max=106751991 day]");
        });
    }

    @Test
    public void testWidestNanosFrameCompilesAndTheNextOneDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tab VALUES ('1970-01-01T00:00:00.000000000Z', 1), ('2261-01-01T00:00:00.000000000Z', 2)");

            // 106751 days is the widest nanosecond frame that fits, and it reaches back over
            // the whole 291 years the two rows span: the second row sees both.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 106751 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000000Z\t1.0
                            2261-01-01T00:00:00.000000000Z\t3.0
                            """);
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN 106752 DAY PRECEDING AND CURRENT ROW) FROM tab")
                    .noLeakCheck()
                    .fails(50, "RANGE frame start is out of range for the designated timestamp [width=106752 day, max=106751 day]");
        });
    }

    @Test
    public void testWidestNanosFrameEndCompilesAndTheNextOneDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tab VALUES ('1970-01-01T00:00:00.000000000Z', 1), ('2261-01-01T00:00:00.000000000Z', 2)");

            // 100000 days - 273 years - is a lagging end bound nanos do carry: the 2261 row's
            // frame stops in the year 1987 and still reaches the 1970 row.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 100000 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000000Z\tnull
                            2261-01-01T00:00:00.000000000Z\t1.0
                            """);
            // The widest end bound nanos carry reaches back past the 291 years the rows span, so
            // both frames are empty - the point is that it compiles at all.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 106751 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000000Z\tnull
                            2261-01-01T00:00:00.000000000Z\tnull
                            """);
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 106752 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(74, "RANGE frame end is out of range for the designated timestamp [width=106752 day, max=106751 day]");
        });
    }
}
