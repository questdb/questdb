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

package io.questdb.test.cairo.covering;

import org.junit.Test;

/**
 * A SAMPLE BY over an unordered base must obtain ascending designated-timestamp order before it
 * builds buckets. The base here is a multi-key covering {@code latestBy}: it emits one row per key
 * in key order and honestly reports {@code SCAN_DIRECTION_OTHER}.
 * <p>
 * The fail-safe scan-direction change sorts such a base at the SAMPLE BY gate. These tests compare
 * every serial SAMPLE BY variant with the ordered full-scan route and pin the fallback sort in the
 * plan, so an unordered base can neither be rejected unnecessarily nor produce silently wrong
 * buckets.
 */
public class CoveringIndexSampleByFallbackTest extends AbstractCoveringIndexQueryTest {

    /**
     * Two keys are the minimum: single-key {@code latestBy} returns one row and is trivially
     * ordered, which would make the fallback assertions vacuous.
     */
    private static final String UNORDERED_BASE =
            "(SELECT * FROM telemetry WHERE param_id IN ('SFID','HOTMIC') LATEST ON ts PARTITION BY param_id)";

    @Test
    public void testSampleByAlignToCalendarSortsUnorderedBase() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertSortedFallback("first(value)", " SAMPLE BY 10s ALIGN TO CALENDAR");
        });
    }

    @Test
    public void testSampleByAlignToFirstObservationSortsUnorderedBase() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertSortedFallback("first(value)", " SAMPLE BY 10s ALIGN TO FIRST OBSERVATION");
        });
    }

    @Test
    public void testSampleByFillSortsUnorderedBase() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertSortedFallback("first(value)", " SAMPLE BY 10s FILL(NULL)");
        });
    }

    /**
     * The fallback is a property of the base requirement, not of aggregate order-sensitivity:
     * {@code count()} needs the same timestamp order to form the right buckets.
     */
    @Test
    public void testSampleByOrderInvariantAggregateSortsUnorderedBase() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertSortedFallback("count()", " SAMPLE BY 10s");
        });
    }

    @Test
    public void testSampleBySortsUnorderedBaseAndReturnsCorrectBuckets() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            final String sql = indexedSql("first(value)", " SAMPLE BY 10s");
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestampAsc("ts")
                    .withPlanContaining("Encode sort", "keys: [ts]")
                    .returns(
                            "ts\tfirst\n" +
                                    "1970-01-01T02:46:30.000000Z\t9997.0\n" +
                                    "1970-01-01T02:46:40.000000Z\t10000.0\n"
                    );
        });
    }

    private void assertSortedFallback(String aggregate, String suffix) throws Exception {
        final String indexed = indexedSql(aggregate, suffix);
        assertSameResult(indexed, referenceSql(aggregate, suffix));
        assertQuery(indexed).assertsPlanContaining("Encode sort", "keys: [ts]");
    }

    private static String indexedSql(String aggregate, String suffix) {
        return "SELECT ts, " + aggregate + " FROM " + UNORDERED_BASE + suffix;
    }

    private static String referenceSql(String aggregate, String suffix) {
        return "SELECT /*+ no_index */ ts, " + aggregate + " FROM " + UNORDERED_BASE + suffix;
    }
}
