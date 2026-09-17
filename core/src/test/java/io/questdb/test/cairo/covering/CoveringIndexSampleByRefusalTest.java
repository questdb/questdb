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
 * Does a SAMPLE BY over a base that does not deliver designated-timestamp order fail loudly, or
 * does it return silently wrong buckets?
 * <p>
 * Per-key (unordered) frames are never offered to a SAMPLE BY -- a time bucket draws from every
 * key, so the offer is declined at the covering factory -- which leaves exactly one way for an
 * unordered base to arrive underneath one: a MULTI-KEY covering {@code latestBy}. That factory
 * emits one row per key in key order, not timestamp order, and says so through
 * {@code getScanDirection() == SCAN_DIRECTION_OTHER}. It does that on stock master too,
 * independently of the per-key work on this branch.
 * <p>
 * The answer, measured rather than assumed: it fails loudly, at a single choke point in
 * {@code SqlCodeGenerator} that rejects any base whose scan direction is not
 * {@code SCAN_DIRECTION_FORWARD} while the execution context requires a timestamp. The rejection
 * happens during sub-query generation, BEFORE a SAMPLE BY factory of any kind is chosen, so it
 * covers every serial SAMPLE BY factory at once rather than each of them carrying its own check.
 * That is what makes the ALIGN TO / FILL variants below worth asserting: they select different
 * factories and all of them are refused identically.
 */
public class CoveringIndexSampleByRefusalTest extends AbstractCoveringIndexQueryTest {

    private static final String NO_ASC_ORDER = "ASC order over TIMESTAMP column is required but not provided";

    /**
     * The multi-key covering {@code latestBy} sub-query that advertises
     * {@code SCAN_DIRECTION_OTHER}. Two keys are the minimum: single-key {@code latestBy} returns
     * one row and is trivially ordered, so it would make every assertion here vacuous.
     */
    private static final String UNORDERED_BASE =
            "(SELECT * FROM telemetry WHERE param_id IN ('SFID','HOTMIC') LATEST ON ts PARTITION BY param_id)";

    @Test
    public void testSampleByAlignToCalendarOverUnorderedBaseIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(value) FROM " + UNORDERED_BASE + " SAMPLE BY 10s ALIGN TO CALENDAR",
                    -1,
                    NO_ASC_ORDER
            );
        });
    }

    @Test
    public void testSampleByAlignToFirstObservationOverUnorderedBaseIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(value) FROM " + UNORDERED_BASE + " SAMPLE BY 10s ALIGN TO FIRST OBSERVATION",
                    -1,
                    NO_ASC_ORDER
            );
        });
    }

    @Test
    public void testSampleByFillOverUnorderedBaseIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(value) FROM " + UNORDERED_BASE + " SAMPLE BY 10s FILL(NULL)",
                    -1,
                    NO_ASC_ORDER
            );
        });
    }

    /**
     * The refusal is a property of the BASE, not of the aggregate: {@code count()} is entirely
     * order-invariant and is refused just the same. Without this the suite could not tell the
     * timestamp-order check apart from the order-sensitive-aggregate guard, which fires on a
     * different condition and carries a different message.
     */
    @Test
    public void testSampleByOverUnorderedBaseIsRefusedEvenForAnOrderInvariantAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertExceptionNoLeakCheck(
                    "SELECT ts, count() FROM " + UNORDERED_BASE + " SAMPLE BY 10s",
                    -1,
                    NO_ASC_ORDER
            );
        });
    }

    @Test
    public void testSampleByOverUnorderedBaseIsRefusedNotSilentlyWrong() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(value) FROM " + UNORDERED_BASE + " SAMPLE BY 10s",
                    -1,
                    NO_ASC_ORDER
            );
        });
    }

    /**
     * Non-vacuity guard for every refusal above. The same SAMPLE BY over the same rows, with the
     * covering index taken out of the plan, compiles and returns two buckets in ascending
     * timestamp order. So the refusals are the unordered base being rejected, not the query being
     * malformed or empty -- and they pin the exact answer a future fix would have to produce.
     */
    @Test
    public void testTheSameSampleByOverAnOrderedBaseCompilesAndReturnsBuckets() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            assertQuery("SELECT /*+ no_index */ ts, first(value) FROM " + UNORDERED_BASE + " SAMPLE BY 10s")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestampAsc("ts")
                    .returns(
                            "ts\tfirst\n" +
                                    "1970-01-01T02:46:30.000000Z\t9997.0\n" +
                                    "1970-01-01T02:46:40.000000Z\t10000.0\n"
                    );
        });
    }
}
