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

package io.questdb.test.cairo.lv;

import org.junit.Test;

/**
 * Pins the live-view rejection contract for the SUBSAMPLE family: the clause itself and every
 * window function it desugars to - {@code lttb} (both overloads), {@code m4}, {@code minmax},
 * {@code uniform}, {@code cadence} (both overloads) and {@code sdt}.
 * <p>
 * All eight are {@code TWO_PASS} whole-set selectors: pass1 buffers every (ts, value) row, then
 * the selection runs over the complete buffer. That is structurally incompatible with a live
 * view's incremental, bounded-influence refresh model.
 * <p>
 * <b>All eight also have a directly callable {@code OVER (...)} form</b> - not just the keep-flag
 * trio. {@code uniform(100) OVER (ORDER BY ts)}, {@code cadence(100) OVER (ORDER BY ts)} and
 * {@code sdt(ts, v, 0.5) OVER (ORDER BY ts)} all compile outside a live view, so each needs its
 * own reject inside one. The rejects are:
 * <ul>
 *     <li><b>Default frame</b> (the only shape these functions otherwise accept): the
 *     finite-influence rule, {@code SqlParser.validateLiveViewFiniteInfluence}. It keys off the
 *     FRAME, not the function name, so it covers all eight and names the offending function.</li>
 *     <li><b>Bounded frame</b> remedy: every factory rejects framing outright.</li>
 *     <li><b>Anchored named WINDOW</b> remedy: a live-view ANCHOR requires PARTITION BY, and seven
 *     of the eight factories reject PARTITION BY.</li>
 *     <li><b>The SUBSAMPLE clause</b> never reaches any of those: the live-view shape validator
 *     rejects the desugared query model up front.</li>
 * </ul>
 * <b>{@code sdt} is the exception and the fragile case.</b> It is the one factory that SUPPORTS
 * PARTITION BY (see {@code SdtWindowFunctionFactory.SdtOverPartitionFunction}), so the anchored
 * remedy is genuinely available to it and the first three rejects above all miss. What actually
 * stops it is a single, different mechanism - the multi-pass/caching admission gate, which refuses
 * any window function that is not {@code ZERO_PASS}. Unlike the other seven, anchored {@code sdt}
 * is defended in depth by exactly one rule, so
 * {@link #testAnchoredSdtIsHeldBackOnlyByTheMultiPassGuard()} is a tripwire: if {@code sdt} is ever
 * made single-pass (e.g. by narrow-chain work), that test flips and a whole-series compressor
 * silently becomes admissible in an incremental runtime.
 * <p>
 * If any reject here disappears, a live view would compile a whole-history downsampler into an
 * incremental runtime - re-selecting (and re-buffering) unbounded state per refresh cycle - so
 * acceptance must be a deliberate design change, not a validation regression.
 */
public class LiveViewSubsampleRejectTest extends AbstractLiveViewTest {

    private static final String CREATE_PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS ";
    private static final String SELECT_PREFIX = "SELECT ts, v, ";
    /**
     * Factory-raised errors are positioned within the inner SELECT.
     */
    private static final int FACTORY_POS = SELECT_PREFIX.length();
    /**
     * Parser-raised errors are positioned within the whole CREATE statement.
     */
    private static final int PARSER_POS = CREATE_PREFIX.length() + SELECT_PREFIX.length();

    /**
     * Every window-function spelling the SUBSAMPLE family exposes, with the name it reports.
     */
    private static final String[][] ALL_WINDOW_FORMS = {
            {"lttb(ts, v, 100)", "lttb"},
            {"lttb(ts, v, 100, '1h')", "lttb"},
            {"m4(ts, v, 100)", "m4"},
            {"minmax(ts, v, 100)", "minmax"},
            {"uniform(100)", "uniform"},
            {"cadence(100)", "cadence"},
            {"cadence(100, 7)", "cadence"},
            {"sdt(ts, v, 0.5)", "sdt"},
    };

    /**
     * Every method spelling the SUBSAMPLE clause accepts.
     */
    private static final String[] ALL_SUBSAMPLE_METHODS = {
            "lttb(v, 100)",
            "lttb(v, 100, '1h')",
            "m4(v, 100)",
            "minmax(v, 100)",
            "uniform(100)",
            "cadence(100)",
            "cadence(100, 7)",
            "sdt(v, 0.5)",
    };

    /**
     * The anchored remedy the finite-influence reject suggests is unavailable for the seven
     * functions that refuse PARTITION BY - a live-view ANCHOR requires PARTITION BY, so suggesting
     * it leads straight into the factory's reject. sdt is excluded here precisely because it does
     * NOT refuse PARTITION BY; see {@link #testAnchoredSdtIsHeldBackOnlyByTheMultiPassGuard()}.
     */
    @Test
    public void testAnchorRemedyUnavailableForPartitionRefusingFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String[] form : ALL_WINDOW_FORMS) {
                if ("sdt".equals(form[1])) {
                    continue;
                }
                assertException(
                        CREATE_PREFIX + SELECT_PREFIX + form[0] + " OVER w AS keep FROM base " +
                                "WINDOW w AS (PARTITION BY k ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                        FACTORY_POS,
                        form[1] + "() does not support PARTITION BY"
                );
            }
        });
    }

    /**
     * sdt accepts PARTITION BY, so the anchored shape clears the finite-influence rule AND the
     * factory. The only thing left standing between it and an incremental runtime is the
     * multi-pass/caching admission gate. This is the tripwire described in the class doc: if this
     * assertion starts failing because the statement COMPILES, a TWO_PASS whole-series compressor
     * has become admissible in a live view.
     */
    @Test
    public void testAnchoredSdtIsHeldBackOnlyByTheMultiPassGuard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertException(
                    CREATE_PREFIX + SELECT_PREFIX + "sdt(ts, v, 0.5) OVER w AS keep FROM base " +
                            "WINDOW w AS (PARTITION BY k ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                    17,
                    "live view select may only use window functions that support incremental refresh; " +
                            "this query requires caching or multi-pass evaluation"
            );
        });
    }

    /**
     * The other remedy the finite-influence reject suggests - bounding the frame - is unavailable
     * for all eight: every factory rejects ROWS/RANGE framing outright.
     */
    @Test
    public void testBoundedFrameRemedyUnavailableForAllSubsampleFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String[] form : ALL_WINDOW_FORMS) {
                assertException(
                        CREATE_PREFIX + SELECT_PREFIX + form[0] +
                                " OVER (ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS keep FROM base",
                        FACTORY_POS,
                        form[1] + "() does not support framing; remove ROWS/RANGE clause"
                );
                assertException(
                        CREATE_PREFIX + SELECT_PREFIX + form[0] +
                                " OVER (ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) AS keep FROM base",
                        FACTORY_POS,
                        form[1] + "() does not support framing; remove ROWS/RANGE clause"
                );
            }
        });
    }

    /**
     * Materialized views are the other incremental surface these functions must not reach. The
     * window form is refused as a base-table window function; the SUBSAMPLE clause is refused
     * because a matview requires an aggregation interval, which SUBSAMPLE never supplies.
     */
    @Test
    public void testMaterializedViewsRejectSubsampleFamily() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String[] form : ALL_WINDOW_FORMS) {
                assertException(
                        "CREATE MATERIALIZED VIEW mv REFRESH IMMEDIATE AS (" + SELECT_PREFIX + form[0] +
                                " OVER (ORDER BY ts) AS keep FROM base) PARTITION BY DAY",
                        64,
                        "window function on base table is not supported for materialized views: base"
                );
            }
            for (String method : ALL_SUBSAMPLE_METHODS) {
                assertException(
                        "CREATE MATERIALIZED VIEW mv REFRESH IMMEDIATE AS (SELECT ts, v FROM base SUBSAMPLE " +
                                method + ") TIMESTAMP(ts) PARTITION BY DAY",
                        50,
                        "materialized view query requires a sampling interval"
                );
            }
        });
    }

    /**
     * The SUBSAMPLE clause desugars into a window subquery + keep filter, which is not the plain
     * windowed base-table scan live views support. The shape validator turns it away before the
     * finite-influence rule ever sees the desugared window function, for every method.
     */
    @Test
    public void testSubsampleClauseRejectedByLiveViewShapeValidator() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String method : ALL_SUBSAMPLE_METHODS) {
                assertException(
                        CREATE_PREFIX + "SELECT ts, v FROM base SUBSAMPLE " + method,
                        17,
                        "live view select must be a plain windowed scan of the base table; " +
                                "this query shape is not supported yet"
                );
            }
        });
    }

    /**
     * Hiding the window call inside a subquery or CTE does not smuggle it past the gates: a live
     * view requires a plain single-base-table FROM, so the nesting itself is refused.
     */
    @Test
    public void testSubsampleFamilyRejectedWhenNested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String[] form : ALL_WINDOW_FORMS) {
                assertException(
                        CREATE_PREFIX + "SELECT ts, v FROM (" + SELECT_PREFIX + form[0] +
                                " OVER (ORDER BY ts) AS keep FROM base) WHERE keep",
                        53,
                        "live view requires a single base table in FROM clause"
                );
                assertException(
                        CREATE_PREFIX + "WITH d AS (" + SELECT_PREFIX + form[0] +
                                " OVER (ORDER BY ts) AS keep FROM base) SELECT ts, v FROM d WHERE keep",
                        53,
                        "live view requires a single base table in FROM clause"
                );
            }
        });
    }

    /**
     * An anchored live-view WINDOW must carry PARTITION BY, and a bare unbounded window must carry
     * an ANCHOR. Together these close the two half-shapes an sdt author might reach for after
     * discovering that sdt accepts PARTITION BY.
     */
    @Test
    public void testSdtAnchorShapePrerequisitesAreEnforced() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // PARTITION BY without an ANCHOR: still an unbounded window.
            assertException(
                    CREATE_PREFIX + SELECT_PREFIX + "sdt(ts, v, 0.5) OVER (PARTITION BY k ORDER BY ts) AS keep FROM base",
                    102,
                    "live view unbounded window must have an ANCHOR clause"
            );
            // ANCHOR without PARTITION BY: not an admissible anchored window.
            assertException(
                    CREATE_PREFIX + SELECT_PREFIX + "sdt(ts, v, 0.5) OVER w AS keep FROM base " +
                            "WINDOW w AS (ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                    133,
                    "live view anchored WINDOW requires PARTITION BY"
            );
        });
    }

    /**
     * The only window shape these functions accept - default (unbounded preceding) frame over
     * ORDER BY ts with no PARTITION BY - is exactly the shape the finite-influence rule rejects.
     * The rule reads the frame rather than the function name, so it must cover all eight forms and
     * name each one. Covers the position-only functions (uniform/cadence) and sdt, which earlier
     * revisions of this test wrongly assumed had no window-function form.
     */
    @Test
    public void testWindowFormsRejectedByFiniteInfluenceRule() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String[] form : ALL_WINDOW_FORMS) {
                assertException(
                        CREATE_PREFIX + SELECT_PREFIX + form[0] + " OVER (ORDER BY ts) AS keep FROM base",
                        PARSER_POS,
                        "live view select cannot use " + form[1] + "() over a frame starting at UNBOUNDED PRECEDING; " +
                                "it has no finite out-of-order influence boundary, so a late row would replay the whole history"
                );
            }
        });
    }
}
