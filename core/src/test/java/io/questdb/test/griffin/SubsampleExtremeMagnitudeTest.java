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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * LTTB selection must follow exact triangle-area ordering even when the
 * finite input values are so large (or small) that the double-precision area
 * arithmetic {@code |dbx*(avgY-ay) - avgDx*(by-ay)|} leaves the finite range.
 * Overflow produces infinite areas (which tie, so the first candidate wins)
 * or NaN areas via Inf - Inf (which never compare greater, pinning the
 * bucket's first point); either silently drops the true peak. Every expected
 * selection below is derived from exact shoelace arithmetic, documented as
 * doubled areas at each fixture.
 */
public class SubsampleExtremeMagnitudeTest extends AbstractCairoTest {
    private static final String MEAN_OVERFLOW_IDS = """
            id
            1
            3
            5
            6
            """;
    private static final String NAN_AREA_IDS = """
            id
            1
            4
            6
            """;
    private static final String PEAK_IDS = """
            id
            1
            4
            5
            """;
    private static final String PRESELECT_IDS = """
            id
            1
            8
            12
            """;

    @Test
    public void testLttbCompetingLargeAreasSubsample() throws Exception {
        assertCompetingLargeAreas(true);
    }

    @Test
    public void testLttbCompetingLargeAreasWindow() throws Exception {
        assertCompetingLargeAreas(false);
    }

    @Test
    public void testLttbMinMaxPreselectOverflowSubsample() throws Exception {
        assertMinMaxPreselectOverflow(true);
    }

    @Test
    public void testLttbMinMaxPreselectOverflowWindow() throws Exception {
        assertMinMaxPreselectOverflow(false);
    }

    @Test
    public void testLttbNaNAreaSubsample() throws Exception {
        assertNaNArea(true);
    }

    @Test
    public void testLttbNaNAreaWindow() throws Exception {
        assertNaNArea(false);
    }

    @Test
    public void testLttbNextBucketMeanOverflowSubsample() throws Exception {
        assertNextBucketMeanOverflow(true);
    }

    @Test
    public void testLttbNextBucketMeanOverflowWindow() throws Exception {
        assertNextBucketMeanOverflow(false);
    }

    @Test
    public void testLttbSubnormalValuesSubsample() throws Exception {
        assertSubnormalValues(true);
    }

    @Test
    public void testLttbSubnormalValuesWindow() throws Exception {
        assertSubnormalValues(false);
    }

    @Test
    public void testLttbValueDifferenceOverflowSubsample() throws Exception {
        assertValueDifferenceOverflow(true);
    }

    @Test
    public void testLttbValueDifferenceOverflowWindow() throws Exception {
        assertValueDifferenceOverflow(false);
    }

    /**
     * Target 3 pins ids 1 and 5; the middle bucket is {2, 3, 4} and the next
     * bucket collapses to id 5, so with both endpoint values at 0 each doubled
     * area is 4*|v|: id 2 = 4e308, id 3 = 2e308, id 4 = 6e308 - distinct, with
     * a unique winner at id 4, yet all three past Double.MAX_VALUE. Naive
     * arithmetic collapses them into an Infinity tie won by id 2. The rescaled
     * control (identical shape divided by 5e307) must agree.
     */
    private void assertCompetingLargeAreas(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, 1e308, 1),
                    (3, 5e307, 2),
                    (4, 1.5e308, 3),
                    (5, 0.0, 4)
                    """);
            assertQuery(query(isSubsample, 3)).returns(PEAK_IDS);

            execute("TRUNCATE TABLE t");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, 2.0, 1),
                    (3, 1.0, 2),
                    (4, 3.0, 3),
                    (5, 0.0, 4)
                    """);
            assertQuery(query(isSubsample, 3)).returns(PEAK_IDS);
        });
    }

    /**
     * 12 rows activate MinMaxLTTB preselection for target 3 (interior 10 >
     * 2 * 4 * 1). Two bins over ids 2..11 keep extremes {3, 5} and {8, 10}, so
     * the triangle stage sees candidates {1, 3, 5, 8, 10, 12} with next bucket
     * {12}. Doubled areas are 11*|v|: id 3 = 1.1e309, id 5 = 1.1e308, id 8 =
     * 1.65e309 (unique winner), id 10 = 1.1e308. Naive arithmetic hands the
     * Infinity tie to id 3.
     */
    private void assertMinMaxPreselectOverflow(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, 0.0, 1),
                    (3, 1e308, 2),
                    (4, 0.0, 3),
                    (5, -1e307, 4),
                    (6, 0.0, 5),
                    (7, 0.0, 6),
                    (8, 1.5e308, 7),
                    (9, 0.0, 8),
                    (10, -1e307, 9),
                    (11, 0.0, 10),
                    (12, 0.0, 11)
                    """);
            assertQuery(query(isSubsample, 3)).returns(PRESELECT_IDS);
        });
    }

    /**
     * Target 3 pins ids 1 and 6; the middle bucket is {2, 3, 4, 5} against the
     * anchor (0, 0) and next-bucket mean (5, 1.6e308). Doubled areas
     * |dbx * 1.6e308 - 5 * v|: id 2 = 3.4e308, id 3 = 1.8e308, id 4 = 9.8e308
     * (unique winner), id 5 = 6.4e308. In doubles id 2 sees |1.6e308 - Inf| =
     * Inf and wins immediately, while id 3 hits Inf - Inf = NaN, exercising
     * both non-finite poisonings in one bucket.
     */
    private void assertNaNArea(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, 1e308, 1),
                    (3, 1e308, 2),
                    (4, -1e308, 3),
                    (5, 0.0, 4),
                    (6, 1.6e308, 5)
                    """);
            assertQuery(query(isSubsample, 3)).returns(NAN_AREA_IDS);
        });
    }

    /**
     * Target 4 over 6 rows gives bucket {2, 3} whose next bucket {4, 5} holds
     * two 1e308 values: the mean's accumulator saturates at Infinity although
     * the true mean is 1e308. Exact doubled areas |dbx * 1e308 - 3.5 * v|:
     * id 2 = 1.35e308, id 3 = 1.65e308 (winner). The second bucket {4, 5} then
     * overflows too: against anchor (2, 1e307) and next bucket {6}, doubled
     * areas |dbx * 1e307 + 3 * (v - 1e307)|: id 4 = 2.8e308, id 5 = 2.9e308
     * (winner). Naive arithmetic picks ids 2 and 4 through Infinity ties.
     */
    private void assertNextBucketMeanOverflow(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, -1e307, 1),
                    (3, 1e307, 2),
                    (4, 1e308, 3),
                    (5, 1e308, 4),
                    (6, 0.0, 5)
                    """);
            assertQuery(query(isSubsample, 4)).returns(MEAN_OVERFLOW_IDS);
        });
    }

    /**
     * Control at the opposite end of the range: subnormal inputs. Doubled
     * areas 4*|v| (1.6e-319, 8e-320, 2.4e-319) stay representable through
     * gradual underflow, so the ordering is intact with or without the
     * overflow fallback; this pins that behavior.
     */
    private void assertSubnormalValues(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 0.0, 0),
                    (2, 4e-320, 1),
                    (3, 2e-320, 2),
                    (4, 6e-320, 3),
                    (5, 0.0, 4)
                    """);
            assertQuery(query(isSubsample, 3)).returns(PEAK_IDS);
        });
    }

    /**
     * Both endpoints sit at -1.7e308, so the anchor-relative differences
     * v + 1.7e308 overflow before any product is formed. Doubled areas
     * 4 * (v + 1.7e308): id 2 = 1.32e309, id 3 = 6.8e308, id 4 = 1.36e309
     * (unique winner). Naive arithmetic gives id 2 the Infinity tie.
     */
    private void assertValueDifferenceOverflow(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, -1.7e308, 0),
                    (2, 1.6e308, 1),
                    (3, 0.0, 2),
                    (4, 1.7e308, 3),
                    (5, -1.7e308, 4)
                    """);
            assertQuery(query(isSubsample, 3)).returns(PEAK_IDS);
        });
    }

    private static String query(boolean isSubsample, int target) {
        if (isSubsample) {
            return "SELECT id FROM (SELECT id, v, ts FROM t SUBSAMPLE lttb(v, " + target + "))";
        }
        return "SELECT id FROM (SELECT id, lttb(ts, v, " + target + ") OVER (ORDER BY ts) AS keep FROM t) WHERE keep";
    }
}
