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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A RANGE frame bound carries a time unit, and the runtime converts it into the designated
 * timestamp's own units before it builds the frame. That conversion multiplies by a per-unit
 * constant without checking the result, so a width the units cannot carry used to come back as
 * a different width - and the query then computed over a frame nobody wrote, silently.
 * <p>
 * These tests cover the three ways it went wrong, one per shape, on both timestamp resolutions,
 * and pin the widest width that must keep compiling. The widths involved are far outside real
 * time-series use; what makes them worth a test is that two of the three failed without a word.
 */
public class WindowRangeFrameOverflowTest extends AbstractCairoTest {

    @Test
    public void testFrameEndOutOfRangeIsNamedAtItsOwnPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, j LONG) TIMESTAMP(ts) PARTITION BY YEAR");

            // The high bound runs through the same conversion, and its error names the end of
            // the frame rather than the start.
            assertQuery("SELECT ts, sum(j) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 300000 DAY PRECEDING) FROM tab")
                    .noLeakCheck()
                    .fails(85, "RANGE frame end is out of range for the designated timestamp [width=300000d, max=106751d]");
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
                    .fails(65, "RANGE frame start is out of range for the designated timestamp [width=4294967296d, max=106751d]");
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
                    .fails(61, "RANGE frame start is out of range for the designated timestamp [width=300000d, max=106751d]");
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
                    .fails(61, "RANGE frame start is out of range for the designated timestamp [width=200000d, max=106751d]");
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
                    .fails(64, "RANGE frame start is out of range for the designated timestamp [width=106751992d, max=106751991d]");
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
                    .fails(61, "RANGE frame start is out of range for the designated timestamp [width=106752d, max=106751d]");
        });
    }
}
